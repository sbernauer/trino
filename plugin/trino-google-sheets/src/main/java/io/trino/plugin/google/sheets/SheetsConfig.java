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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class SheetsConfig
{
    private String credentialsFilePath;
    private String metadataSheetId;
    private int sheetsDataMaxCacheSize = 1000;
    private Duration sheetsDataExpireAfterWrite = new Duration(5, TimeUnit.MINUTES);
    private Duration connectionTimeout = new Duration(5, TimeUnit.MINUTES);
    private Duration readTimeout = new Duration(5, TimeUnit.MINUTES);
    private Duration writeTimeout = new Duration(5, TimeUnit.MINUTES);

    @NotNull
    @FileExists
    public String getCredentialsFilePath()
    {
        return credentialsFilePath;
    }

    @Config("credentials-path")
    @ConfigDescription("Credential file path to google service account")
    public SheetsConfig setCredentialsFilePath(String credentialsFilePath)
    {
        this.credentialsFilePath = credentialsFilePath;
        return this;
    }

    @NotNull
    public String getMetadataSheetId()
    {
        return metadataSheetId;
    }

    @Config("metadata-sheet-id")
    @ConfigDescription("Metadata sheet id containing table sheet mapping")
    public SheetsConfig setMetadataSheetId(String metadataSheetId)
    {
        this.metadataSheetId = metadataSheetId;
        return this;
    }

    @Min(1)
    public int getSheetsDataMaxCacheSize()
    {
        return sheetsDataMaxCacheSize;
    }

    @Config("sheets-data-max-cache-size")
    @ConfigDescription("Sheet data max cache size")
    public SheetsConfig setSheetsDataMaxCacheSize(int sheetsDataMaxCacheSize)
    {
        this.sheetsDataMaxCacheSize = sheetsDataMaxCacheSize;
        return this;
    }

    @MinDuration("1m")
    public Duration getSheetsDataExpireAfterWrite()
    {
        return sheetsDataExpireAfterWrite;
    }

    @Config("sheets-data-expire-after-write")
    @ConfigDescription("Sheets data expire after write duration")
    public SheetsConfig setSheetsDataExpireAfterWrite(Duration sheetsDataExpireAfterWriteMinutes)
    {
        this.sheetsDataExpireAfterWrite = sheetsDataExpireAfterWriteMinutes;
        return this;
    }

    @MinDuration("10s")
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("connection-timeout")
    @ConfigDescription("Timeout when connection to Google Sheets API")
    public SheetsConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @MinDuration("1m")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("read-timeout")
    @ConfigDescription("Timeout when reading from Google Sheets API")
    public SheetsConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    @MinDuration("1m")
    public Duration getWriteTimeout()
    {
        return writeTimeout;
    }

    @Config("write-timeout")
    @ConfigDescription("Timeout when writing to Google Sheets API")
    public SheetsConfig setWriteTimeout(Duration writeTimeout)
    {
        this.writeTimeout = writeTimeout;
        return this;
    }
}
