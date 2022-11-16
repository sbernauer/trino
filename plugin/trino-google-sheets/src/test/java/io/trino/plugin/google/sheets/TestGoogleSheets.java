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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static io.trino.plugin.google.sheets.SheetsQueryRunner.createSheetsQueryRunner;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.getTestCredentialsPath;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestGoogleSheets
        extends AbstractTestQueryFramework
{
    private static final String APPLICATION_NAME = "trino google sheets integration test";
    private static final String TEST_SPREADSHEET_NAME = "Trino integration test";

    private Sheets sheetsService;
    private String spreadsheetId;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sheetsService = getSheetsService();
        spreadsheetId = createSpreadsheetWithTestdata();
        return createSheetsQueryRunner(ImmutableMap.of(), ImmutableMap.of(
                "gsheets.metadata-sheet-id", spreadsheetId + "#Metadata",
                "gsheets.connection-timeout", "1m",
                "gsheets.read-timeout", "1m",
                "gsheets.write-timeout", "1m"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        Drive driveService = getDriveService();
        // As the Google Sheets API does not support deleting spreadsheets, the Drive API needs to be used
        // Currently fails with
        // Access Not Configured. Drive API has not been used in project 1006290340336 before or it is disabled. Enable it by visiting https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project=1006290340336 then retry. If you enabled this API recently, wait a few minutes for the action to propagate to our systems and retry
        // TODO Enable Drive access for Trino project
        // driveService.files().delete(spreadsheetId).execute();
        spreadsheetId = null;
    }

    @Test
    public void testListTable()
    {
        assertQuery("show tables", "SELECT * FROM (VALUES 'metadata_table', 'number_text', 'table_with_duplicate_and_missing_column_names','nation_insert_test')");
        assertQueryReturnsEmptyResult("SHOW TABLES IN gsheets.information_schema LIKE 'number_text'");
        assertQuery("select table_name from gsheets.information_schema.tables WHERE table_schema <> 'information_schema'", "SELECT * FROM (VALUES 'metadata_table', 'number_text', 'table_with_duplicate_and_missing_column_names','nation_insert_test')");
        assertQuery("select table_name from gsheets.information_schema.tables WHERE table_schema <> 'information_schema' LIMIT 1000", "SELECT * FROM (VALUES 'metadata_table', 'number_text', 'table_with_duplicate_and_missing_column_names','nation_insert_test')");
        assertEquals(getQueryRunner().execute("select table_name from gsheets.information_schema.tables WHERE table_schema = 'unknown_schema'").getRowCount(), 0);
    }

    @Test
    public void testDescTable()
    {
        assertQuery("desc number_text", "SELECT * FROM (VALUES('number','bigint','',''), ('text','varchar','',''))");
        assertQuery("desc metadata_table", "SELECT * FROM (VALUES('table name','varchar','',''), ('sheet id','varchar','',''), "
                + "('owner','varchar','',''), ('notes','varchar','',''), ('column types','varchar','',''))");
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM number_text", "SELECT 5");
        assertQuery("SELECT number FROM number_text", "SELECT * FROM (VALUES 1,2,3,4,5)");
        assertQuery("SELECT text FROM number_text", "SELECT * FROM (VALUES 'one','two','three','four','five')");
        assertQuery("SELECT * FROM number_text", "SELECT * FROM (VALUES (1,'one'), (2,'two'), (3,'three'), (4,'four'), (5,'five'))");
    }

    @Test
    public void testSelectFromTableIgnoreCase()
    {
        assertQuery("SELECT count(*) FROM NUMBER_TEXT", "SELECT 5");
        assertQuery("SELECT number FROM Number_Text", "SELECT * FROM (VALUES 1,2,3,4,5)");
    }

    @Test
    public void testQueryingUnknownSchemaAndTable()
    {
        assertQueryFails("select * from gsheets.foo.bar", "line 1:15: Schema 'foo' does not exist");
        assertQueryFails("select * from gsheets.default.foo_bar_table", "Sheet expression not found for table foo_bar_table");
    }

    @Test
    public void testTableWithRepeatedAndMissingColumnNames()
    {
        assertQuery("desc table_with_duplicate_and_missing_column_names", "SELECT * FROM (VALUES('a','varchar','','')," +
                " ('column_1','varchar','',''), ('column_2','varchar','',''), ('c','varchar','',''))");
    }

    @Test
    public void testInsertIntoTable()
            throws Exception
    {
        assertUpdate("INSERT INTO nation_insert_test SELECT * FROM tpch.sf1.nation", 25);

        // We need to wait for Google Sheets API to propagate changes
        Thread.sleep(20000);

        for (int i = 0; i < 5; i++) {
            MaterializedResult result = getQueryRunner().execute(
                    "SELECT COUNT(*) FROM (SELECT * FROM tpch.sf1.nation EXCEPT SELECT * FROM nation_insert_test)");
            if ((long) result.getOnlyValue() == 0) {
                return;
            }
            // Sleep for the cache duration + 5s to be on the safe side
            Thread.sleep(65000);
        }
        fail("INSERT did not propagate");
    }

    private String createSpreadsheetWithTestdata()
            throws IOException
    {
        Spreadsheet spreadsheet = new Spreadsheet()
                .setProperties(new SpreadsheetProperties().setTitle(TEST_SPREADSHEET_NAME))
                .setSheets(ImmutableList.of(
                        new Sheet().setProperties(new SheetProperties().setTitle("Metadata")),
                        new Sheet().setProperties(new SheetProperties().setTitle("Number Text")),
                        new Sheet().setProperties(new SheetProperties().setTitle("Table with duplicate and missing column names")),
                        new Sheet().setProperties(new SheetProperties().setTitle("Nation Insert test"))));

        spreadsheet = sheetsService.spreadsheets().create(spreadsheet).setFields("spreadsheetId").execute();
        String spreadsheetId = spreadsheet.getSpreadsheetId();

        ValueRange updateValues = new ValueRange().setValues(ImmutableList.of(ImmutableList.of(
                "Table Name", "Sheet ID", "Owner", "Notes", "Column types"
        ), ImmutableList.of(
                "metadata_table", spreadsheetId + "#Metadata", "", "Self reference to this sheet as table", ""
        ), ImmutableList.of(
                "number_text", spreadsheetId + "#Number Text", "alice", "Table to test type mapping", "number=bigint"
        ), ImmutableList.of(
                "table_with_duplicate_and_missing_column_names", spreadsheetId + "#Table with duplicate and missing column names", "bob", "Table to test behaviour with duplicate columns", ""
        ), ImmutableList.of(
                "nation_insert_test", spreadsheetId + "#Nation Insert test", "", "Table containing tpch nation table to test inserts", "nationkey=bigint,name=varchar,regionkey=bigint,comment=varchar")));
        UpdateValuesResponse updateResult = sheetsService.spreadsheets().values().update(spreadsheetId, "Metadata", updateValues)
                .setValueInputOption("RAW")
                .execute();
        assertEquals(toIntExact(updateResult.getUpdatedRows()), 5);

        updateValues = new ValueRange().setValues(ImmutableList.of(ImmutableList.of(
                "number", "text"
        ), ImmutableList.of(
                "1", "one"
        ), ImmutableList.of(
                "2", "two"
        ), ImmutableList.of(
                "3", "three"
        ), ImmutableList.of(
                "4", "four"
        ), ImmutableList.of(
                "5", "five")));
        updateResult = sheetsService.spreadsheets().values().update(spreadsheetId, "Number Text", updateValues)
                .setValueInputOption("RAW")
                .execute();
        assertEquals(toIntExact(updateResult.getUpdatedRows()), 6);

        updateValues = new ValueRange().setValues(ImmutableList.of(ImmutableList.of(
                "a", "A", "", "C"
        ), ImmutableList.of(
                "1", "2", "3", "4")));
        updateResult = sheetsService.spreadsheets().values().update(spreadsheetId, "Table with duplicate and missing column names", updateValues)
                .setValueInputOption("RAW")
                .execute();
        assertEquals(toIntExact(updateResult.getUpdatedRows()), 2);

        updateValues = new ValueRange().setValues(ImmutableList.of(ImmutableList.of("nationkey", "name", "regionkey", "comment")));
        updateResult = sheetsService.spreadsheets().values().update(spreadsheetId, "Nation Insert test", updateValues)
                .setValueInputOption("RAW")
                .execute();
        assertEquals(toIntExact(updateResult.getUpdatedRows()), 1);

        return spreadsheetId;
    }

    private Sheets getSheetsService()
            throws Exception
    {
        return new Sheets.Builder(newTrustedTransport(),
                JacksonFactory.getDefaultInstance(),
                getCredentials())
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    private Drive getDriveService()
            throws Exception
    {
        return new Drive.Builder(newTrustedTransport(),
                JacksonFactory.getDefaultInstance(),
                getCredentials())
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    private GoogleCredential getCredentials()
            throws Exception
    {
        String credentialsPath = getTestCredentialsPath();
        return GoogleCredential.fromStream(new FileInputStream(credentialsPath))
                .createScoped(ImmutableList.of(SheetsScopes.SPREADSHEETS, SheetsScopes.DRIVE));
    }
}
