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
package io.trino.plugin.geospatial;

import com.google.common.collect.ImmutableList;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.IspResponse;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Optional;

public class MaxmindFunctions
{
    private static final String MAXMIND_CITY_DATABASE_FILE = "/tmp/GeoIP2-City.mmdb";
    private static final String MAXMIND_ISP_DATABASE_FILE = "/tmp/GeoIP2-ISP.mmdb";

    private static DatabaseReader cityReader;
    private static DatabaseReader ispReader;

    private MaxmindFunctions()
    {
    }

    private static RowType rowType;
    private static PageBuilder pageBuilder;
    private static BlockBuilder blockBuilder;
    private static BlockBuilder entryBlockBuilder;

    @ScalarFunction("maxmind_country")
    @Description("Queries the maxmind database and returns information about the country")
    @SqlNullable
    @SqlType("row(" +
            "country_iso_code " + StandardTypes.VARCHAR + "," +
            "country_name " + StandardTypes.VARCHAR + "," +
            "country_is_in_european_union " + StandardTypes.BOOLEAN +
            ")")
    public static Block maxmind_country(
            @SqlType(StandardTypes.IPADDRESS) Slice ip)
            throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByAddress(ip.getBytes());
        Optional<CityResponse> cityResponse = getCityResponse(ipAddress);
        if (cityResponse.isEmpty()) {
            return null;
        }
        createBlockBuilders(ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR, BooleanType.BOOLEAN));

        addStringRowField(cityResponse.get().getCountry().getIsoCode());
        addStringRowField(cityResponse.get().getCountry().getName());
        addBooleanRowField(cityResponse.get().getCountry().isInEuropeanUnion());

        return finishBlock();
    }

    @ScalarFunction("maxmind_city")
    @Description("Queries the maxmind database and returns information about the city")
    @SqlNullable
    @SqlType("row(" +
            "city_name " + StandardTypes.VARCHAR + "," +
            "city_geo_name_id " + StandardTypes.BIGINT + "," +
            "city_postal_code " + StandardTypes.VARCHAR +
            ")")
    public static Block maxmind_city(
            @SqlType(StandardTypes.IPADDRESS) Slice ip)
            throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByAddress(ip.getBytes());
        Optional<CityResponse> cityResponse = getCityResponse(ipAddress);
        if (cityResponse.isEmpty()) {
            return null;
        }

        createBlockBuilders(ImmutableList.of(VarcharType.VARCHAR, BigintType.BIGINT, VarcharType.VARCHAR));

        addStringRowField(cityResponse.get().getCity().getName());
        addBigIntRowField(cityResponse.get().getCity().getGeoNameId());
        addStringRowField(cityResponse.get().getPostal().getCode());

        return finishBlock();
    }

    @ScalarFunction("maxmind_country_and_city")
    @Description("Queries the maxmind database and returns information about the country and city")
    @SqlNullable
    @SqlType("row(" +
            "country_iso_code " + StandardTypes.VARCHAR + "," +
            "country_name " + StandardTypes.VARCHAR + "," +
            "country_is_in_european_union " + StandardTypes.BOOLEAN + "," +
            "city_name " + StandardTypes.VARCHAR + "," +
            "city_geo_name_id " + StandardTypes.BIGINT + "," +
            "city_postal_code " + StandardTypes.VARCHAR +
            ")")
    public static Block maxmind_country_and_city(
            @SqlType(StandardTypes.IPADDRESS) Slice ip)
            throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByAddress(ip.getBytes());
        Optional<CityResponse> cityResponse = getCityResponse(ipAddress);
        if (cityResponse.isEmpty()) {
            return null;
        }

        createBlockBuilders(ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR, BooleanType.BOOLEAN,
                VarcharType.VARCHAR, BigintType.BIGINT, VarcharType.VARCHAR));

        addStringRowField(cityResponse.get().getCountry().getIsoCode());
        addStringRowField(cityResponse.get().getCountry().getName());
        addBooleanRowField(cityResponse.get().getCountry().isInEuropeanUnion());
        addStringRowField(cityResponse.get().getCity().getName());
        addBigIntRowField(cityResponse.get().getCity().getGeoNameId());
        addStringRowField(cityResponse.get().getPostal().getCode());

        return finishBlock();
    }

    @ScalarFunction("maxmind_city_full_json")
    @Description("Queries the maxmind database and returns a json document containing all the information about the city")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice maxmind_city_full_json(
            @SqlType(StandardTypes.IPADDRESS) Slice ip)
            throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByAddress(ip.getBytes());
        Optional<CityResponse> cityResponse = getCityResponse(ipAddress);
        if (cityResponse.isEmpty()) {
            return null;
        }

        return Slices.utf8Slice(cityResponse.get().toJson());
    }

    @ScalarFunction("maxmind_isp")
    @Description("Queries the maxmind database and returns information about the ISP")
    @SqlNullable
    @SqlType("row(" +
            "isp_name " + StandardTypes.VARCHAR + "," +
            "isp_organization " + StandardTypes.VARCHAR + "," +
            "isp_as_number " + StandardTypes.BIGINT + "," +
            "isp_as_organization " + StandardTypes.VARCHAR +
            ")")
    public static Block maxmind_isp(
            @SqlType(StandardTypes.IPADDRESS) Slice ip)
            throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByAddress(ip.getBytes());
        Optional<IspResponse> ispResponse = getIspResponse(ipAddress);
        if (ispResponse.isEmpty()) {
            return null;
        }

        createBlockBuilders(ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR, BigintType.BIGINT, VarcharType.VARCHAR));

        addStringRowField(ispResponse.get().getIsp());
        addStringRowField(ispResponse.get().getOrganization());
        addBigIntRowField(ispResponse.get().getAutonomousSystemNumber());
        addStringRowField(ispResponse.get().getAutonomousSystemOrganization());

        return finishBlock();
    }

    @ScalarFunction("maxmind_isp_full_json")
    @Description("Queries the maxmind database and returns a json document containing all the information about the ISP")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice maxmind_isp_full_json(
            @SqlType(StandardTypes.IPADDRESS) Slice ip)
            throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByAddress(ip.getBytes());
        Optional<IspResponse> ispResponse = getIspResponse(ipAddress);
        if (ispResponse.isEmpty()) {
            return null;
        }

        return Slices.utf8Slice(ispResponse.get().toJson());
    }

    private static void createBlockBuilders(List<Type> types)
    {
        rowType = RowType.anonymous(types);
        pageBuilder = new PageBuilder(ImmutableList.of(rowType));
        blockBuilder = pageBuilder.getBlockBuilder(0);
        entryBlockBuilder = blockBuilder.beginBlockEntry();
    }

    private static Block finishBlock()
    {
        blockBuilder.closeEntry();
        pageBuilder.declarePosition();

        return rowType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    private static void addStringRowField(String value)
    {
        if (value == null) {
            entryBlockBuilder.appendNull();
        }
        else {
            VarcharType.VARCHAR.writeString(entryBlockBuilder, value);
        }
    }

    private static void addBigIntRowField(Integer value)
    {
        if (value == null) {
            entryBlockBuilder.appendNull();
        }
        else {
            BigintType.BIGINT.writeLong(entryBlockBuilder, (long) value);
        }
    }

    private static void addBooleanRowField(boolean value)
    {
        BooleanType.BOOLEAN.writeBoolean(entryBlockBuilder, value);
    }

    private static Optional<CityResponse> getCityResponse(InetAddress ipAddress)
            throws IOException, GeoIp2Exception
    {
        if (cityReader == null) {
            File database = new File(MAXMIND_CITY_DATABASE_FILE);
            cityReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
        }
        return cityReader.tryCity(ipAddress);
    }

    private static Optional<IspResponse> getIspResponse(InetAddress ipAddress)
            throws IOException, GeoIp2Exception
    {
        if (ispReader == null) {
            File database = new File(MAXMIND_ISP_DATABASE_FILE);
            ispReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
        }
        return ispReader.tryIsp(ipAddress);
    }
}
