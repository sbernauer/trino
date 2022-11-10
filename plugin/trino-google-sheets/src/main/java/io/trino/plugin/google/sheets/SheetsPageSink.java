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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_COLUMN_TYPE_NOT_SUPPORTED;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SheetsPageSink
        implements ConnectorPageSink
{
    private final SheetsClient sheetsClient;
    private final String table;
    private final List<SheetsColumnHandle> columns;

    public SheetsPageSink(SheetsClient sheetsClient, String table,
                          List<SheetsColumnHandle> columns)
    {
        this.sheetsClient = sheetsClient;
        this.table = table;
        this.columns = columns;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        String sheetExpression = sheetsClient.getCachedSheetExpressionForTable(table);
        List<List<Object>> rows = new ArrayList<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> row = new ArrayList<>();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                row.add(getObjectValue(columns.get(channel).getColumnType(),
                        page.getBlock(channel), position));
            }
            rows.add(row);
        }
        sheetsClient.insertIntoSheet(sheetExpression, rows);
        return NOT_BLOCKED;
    }

    private Object getObjectValue(Type type, Block block, int position)
    {
        if (type.equals(BigintType.BIGINT)) {
            return type.getLong(block, position);
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return type.getSlice(block, position).toStringUtf8();
        }
        throw new TrinoException(SHEETS_COLUMN_TYPE_NOT_SUPPORTED, "Unsupported type " + type + " when writing to Google sheets tables");
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}
