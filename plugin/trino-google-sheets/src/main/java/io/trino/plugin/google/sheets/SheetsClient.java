/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.google.sheets;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.spi.TrinoException;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_INSERT_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_METASTORE_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_TABLE_LOAD_ERROR;
import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SheetsClient
{
    private static final Logger log = Logger.get(SheetsClient.class);

    private static final String APPLICATION_NAME = "trino google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String COLUMN_TYPE_CACHE_KEY = "_column_type_";
    private static final String INSERT_VALUE_OPTION = "RAW";
    private static final String DELIMITER_HASH = "#";
    private static final String DELIMITER_COMMA = ",";
    private static final String DELIMITER_EQUALS = "=";

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS);

    private final NonEvictableLoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final LoadingCache<String, List<List<Object>>> sheetDataCache;

    private final String metadataSheetId;
    private final String credentialsFilePath;

    private final Sheets sheetsService;

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec)
    {
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.metadataSheetId = config.getMetadataSheetId();
        this.credentialsFilePath = config.getCredentialsFilePath();

        try {
            this.sheetsService = new Sheets.Builder(newTrustedTransport(), JSON_FACTORY, getCredentials()).setApplicationName(APPLICATION_NAME).build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
        long expiresAfterWriteMillis = config.getSheetsDataExpireAfterWrite().toMillis();
        long maxCacheSize = config.getSheetsDataMaxCacheSize();

        this.tableSheetMappingCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).maximumSize(maxCacheSize),
                new CacheLoader<>()
                {
                    @Override
                    public Optional<String> load(String tableName)
                    {
                        return getSheetExpressionForTable(tableName);
                    }

                    @Override
                    public Map<String, Optional<String>> loadAll(Iterable<? extends String> tableList)
                    {
                        return getAllTableSheetExpressionMapping();
                    }
                });

        this.sheetDataCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
                .maximumSize(maxCacheSize)
                .build(CacheLoader.from(this::readAllValuesFromSheetExpression));
    }

    public Optional<SheetsTable> getTable(String tableName)
    {
        List<List<String>> values = convertToStringValues(readAllValues(tableName));
        if (values.size() > 0) {
            ImmutableList.Builder<SheetsColumn> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            List<String> header = values.get(0);
            int count = 0;
            for (String column : header) {
                String columnValue = column.toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                Optional<String> columType = tableSheetMappingCache.getIfPresent(tableName + COLUMN_TYPE_CACHE_KEY + columnValue);
                if (columType != null && columType.isPresent()) {
                    columns.add(new SheetsColumn(columnValue, SheetsHelper.getType(columType.get())));
                }
                else {
                    // If no type was specified for this column in the metadata table VARCHAR type is used to read the data
                    columns.add(new SheetsColumn(columnValue, VarcharType.VARCHAR));
                }
            }
            List<List<String>> dataValues = values.subList(1, values.size()); // removing header info
            return Optional.of(new SheetsTable(tableName, columns.build(), dataValues));
        }
        return Optional.empty();
    }

    public Set<String> getTableNames()
    {
        ImmutableSet.Builder<String> tables = ImmutableSet.builder();
        try {
            List<List<Object>> tableMetadata = sheetDataCache.getUnchecked(metadataSheetId);
            for (int i = 1; i < tableMetadata.size(); i++) {
                if (tableMetadata.get(i).size() > 0) {
                    tables.add(String.valueOf(tableMetadata.get(i).get(0)));
                }
            }
            return tables.build();
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(SHEETS_METASTORE_ERROR, e);
        }
    }

    public List<List<Object>> readAllValues(String tableName)
    {
        try {
            String sheetExpression = getCachedSheetExpressionForTable(tableName);
            return sheetDataCache.getUnchecked(sheetExpression);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(SHEETS_TABLE_LOAD_ERROR, "Error loading data for table: " + tableName, e);
        }
    }

    public static List<List<String>> convertToStringValues(List<List<Object>> values)
    {
        return values.stream()
                .map(columns -> columns.stream().map(String::valueOf).collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private Optional<String> getSheetExpressionForTable(String tableName)
    {
        Map<String, Optional<String>> tableSheetMap = getAllTableSheetExpressionMapping();
        if (!tableSheetMap.containsKey(tableName)) {
            return Optional.empty();
        }
        return tableSheetMap.get(tableName);
    }

    private Map<String, Optional<String>> getAllTableSheetExpressionMapping()
    {
        ImmutableMap.Builder<String, Optional<String>> tableSheetMap = ImmutableMap.builder();
        List<List<Object>> data = readAllValuesFromSheetExpression(metadataSheetId);
        // first line is assumed to be sheet header
        for (int i = 1; i < data.size(); i++) {
            if (data.get(i).size() >= 2) {
                String tableId = String.valueOf(data.get(i).get(0));
                String sheetId = String.valueOf(data.get(i).get(1));
                tableSheetMap.put(tableId.toLowerCase(Locale.ENGLISH), Optional.of(sheetId));

                if (data.get(i).size() >= 5) {
                    String[] tableColumnTypes = String.valueOf(data.get(i).get(4)).split(DELIMITER_COMMA);
                    for (String tableColumnType : tableColumnTypes) {
                        String[] split = tableColumnType.split(DELIMITER_EQUALS);
                        String columnName = split[0];
                        String columnType = split[1];
                        tableSheetMappingCache.put(tableId + COLUMN_TYPE_CACHE_KEY + columnName, Optional.of(columnType));
                    }
                }
            }
        }
        return tableSheetMap.buildOrThrow();
    }

    private Credential getCredentials()
    {
        try (InputStream in = new FileInputStream(credentialsFilePath)) {
            return GoogleCredential.fromStream(in).createScoped(SCOPES);
        }
        catch (IOException e) {
            throw new TrinoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    public String getCachedSheetExpressionForTable(String tableName)
    {
        Optional<String> sheetExpression = tableSheetMappingCache.getUnchecked(tableName);
        if (sheetExpression.isEmpty()) {
            throw new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName);
        }
        return sheetExpression.get();
    }

    public String[] extractSheetIdAndRange(String sheetExpression)
    {
        // by default loading up to 10k rows from the first tab of the sheet
        String range = "$1:$10000";
        String[] tableOptions = sheetExpression.split(DELIMITER_HASH);
        String sheetId = tableOptions[0];
        if (tableOptions.length > 1) {
            range = tableOptions[1];
        }

        return new String[] {sheetId, range};
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression)
    {
        String[] sheetIdAndRange = extractSheetIdAndRange(sheetExpression);
        try {
            log.debug("Accessing sheet id [%s] with range [%s]", sheetIdAndRange[0], sheetIdAndRange[1]);
            return sheetsService.spreadsheets().values().get(sheetIdAndRange[0], sheetIdAndRange[1]).execute().getValues();
        }
        catch (IOException e) {
            throw new TrinoException(SHEETS_UNKNOWN_TABLE_ERROR, "Failed reading data from sheet: " + sheetExpression, e);
        }
    }

    public Integer insertIntoSheet(String sheetExpression, List<List<Object>> rows)
    {
        ValueRange body = new ValueRange().setValues(rows);
        AppendValuesResponse result;
        String[] sheetIdAndRange = extractSheetIdAndRange(sheetExpression);
        try {
            result = sheetsService.spreadsheets().values().append(sheetIdAndRange[0], sheetIdAndRange[1], body)
                    .setValueInputOption(INSERT_VALUE_OPTION)
                    .execute();
        }
        catch (IOException e) {
            throw new TrinoException(SHEETS_INSERT_ERROR, "Error inserting data to sheet: ", e);
        }

        // Flush the cache contents for the table that was written to.
        // This is a best-effort solution, since the Google Sheets API seems to be eventually consistent.
        // If the table written to will be queried directly afterwards the inserts might not have been propagated yet
        // and the users needs to wait till the cached version alters out.
        flushSheetDataCache(sheetExpression);
        return result.getUpdates().getUpdatedCells();
    }

    private void flushSheetDataCache(String sheetExpression)
    {
        sheetDataCache.invalidate(sheetExpression);
    }
}
