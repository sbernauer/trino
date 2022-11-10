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

import io.trino.spi.TrinoException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import static io.trino.plugin.google.sheets.SheetsErrorCode.SHEETS_COLUMN_TYPE_NOT_SUPPORTED;

public final class SheetsHelper
{
    private SheetsHelper() {}

    public static Type getType(String type)
    {
        return switch (type) {
            case "bigint" -> BigintType.BIGINT;
            case "varchar" -> VarcharType.VARCHAR;
            default -> throw new TrinoException(SHEETS_COLUMN_TYPE_NOT_SUPPORTED, "Google sheets column type " + type + " is not supported");
        };
    }
}
