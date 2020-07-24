package com.facebook.presto.plugin.clickhouse;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcErrorCode;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.plugin.jdbc.StandardReadMappings;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import static com.facebook.presto.plugin.jdbc.ReadMapping.longReadMapping;
import ru.yandex.clickhouse.ClickHouseDriver;
import org.joda.time.chrono.ISOChronology;

public class ClickhouseClient extends BaseJdbcClient {
    private static final Joiner DOT_JOINER = Joiner.on(".");
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    @Inject
    public ClickhouseClient(JdbcConnectorId connectorId, BaseJdbcConfig config) {
        super(connectorId, config, "\"", new DriverConnectionFactory(new ClickHouseDriver(), config));
        log.info("Creating a Clickhouse Client!");
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata) throws SQLException {
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null), null);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle) {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    int dateType = resultSet.getInt("DATA_TYPE");
                    if (dateType == Types.ARRAY) {
                        dateType = Types.VARCHAR;
                    }
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(dateType, resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    log.info("jdbcTypehandler: " + typeHandle.toString() + ", colname:"
                            + resultSet.getString("COLUMN_NAME") + ", nullable: " + resultSet.getInt("NULLABLE"));
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = ResultSetMetaData.columnNullable == resultSet.getInt("NULLABLE");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle,
                                columnMapping.get().getType(), nullable));
                    }
                    // if (resultSet.getInt("DATA_TYPE") == Types.ARRAY) {
                    // String columnName = resultSet.getString("COLUMN_NAME");
                    // boolean nullable = ResultSetMetaData.columnNullable ==
                    // resultSet.getInt("NULLABLE");
                    // // columns.add(
                    // // new JdbcColumnHandle(connectorId, columnName, typeHandle,
                    // // new com.facebook.presto.spi.type.ArrayType(
                    // // VarcharType.createVarcharType(resultSet.getInt("COLUMN_SIZE"))),
                    // // nullable));
                    // typeHandle = new JdbcTypeHandle(Types.VARCHAR,
                    // resultSet.getInt("COLUMN_SIZE"),
                    // resultSet.getInt("DECIMAL_DIGITS"));
                    // columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle,
                    // VarcharType.createUnboundedVarcharType(), nullable));
                    // }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            throw new PrestoException(JdbcErrorCode.JDBC_ERROR, e);
        }
    }

    public static ReadMapping timestampReadMapping() {
        return longReadMapping(TimestampType.TIMESTAMP, (resultSet, columnIndex) -> {
            /*
             * TODO `resultSet.getTimestamp(columnIndex)` returns wrong value if JVM's zone
             * had forward offset change and the local time corresponding to timestamp value
             * being retrieved was not present (a 'gap'), this includes regular DST changes
             * (e.g. Europe/Warsaw) and one-time policy changes (Asia/Kathmandu's shift by
             * 15 minutes on January 1, 1986, 00:00:00). The problem can be averted by using
             * `resultSet.getObject(columnIndex, LocalDateTime.class)` -- but this is not
             * universally supported by JDBC drivers.
             */
            java.sql.Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            if (timestamp != null) {
                return timestamp.getTime();
            } else {
                return 0;
            }
        });
    }

    public static ReadMapping dateReadMapping() {
        return longReadMapping(DateType.DATE, (resultSet, columnIndex) -> {
            /*
             * JDBC returns a date using a timestamp at midnight in the JVM timezone, or
             * earliest time after that if there was no midnight. This works correctly for
             * all dates and zones except when the missing local times 'gap' is 24h. I.e.
             * this fails when JVM time zone is Pacific/Apia and date to be returned is
             * 2011-12-30.
             *
             * `return resultSet.getObject(columnIndex, LocalDate.class).toEpochDay()`
             * avoids these problems but is currently known not to work with Redshift (old
             * Postgres connector) and SQL Server.
             */
            java.sql.Date date = resultSet.getDate(columnIndex);
            if (date != null) {
                long localMillis = date.getTime();
                // Convert it to a ~midnight in UTC.
                long utcMillis = ISOChronology.getInstance().getZone()
                        .getMillisKeepLocal(org.joda.time.DateTimeZone.UTC, localMillis);
                // convert to days
                return java.util.concurrent.TimeUnit.MILLISECONDS.toDays(utcMillis);
            } else {
                return 0;
            }

        });
    }

    public static ReadMapping timeReadMapping() {
        return longReadMapping(TimeType.TIME, (resultSet, columnIndex) -> {
            /*
             * TODO `resultSet.getTime(columnIndex)` returns wrong value if JVM's zone had
             * forward offset change during 1970-01-01 and the time value being retrieved
             * was not present in local time (a 'gap'), e.g. time retrieved is 00:10:00 and
             * JVM zone is America/Hermosillo The problem can be averted by using
             * `resultSet.getObject(columnIndex, LocalTime.class)` -- but this is not
             * universally supported by JDBC drivers.
             */
            java.sql.Time time = resultSet.getTime(columnIndex);
            if (time != null) {
                return ISOChronology.getInstanceUTC().millisOfDay().get(time.getTime());
            } else {
                return 0;
            }
        });
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle) {

        switch (typeHandle.getJdbcType()) {
            // because in clickhouse-jdbc jar,the date 0000-00-00 or timestamp 0000-00-00
            // 00:00:00 will converts to null, so we can hack the value of
            // date,time,timestamp type, or it will throw the NullPointerException
            case Types.DATE:
                return Optional.of(dateReadMapping());
            case Types.TIME:
                return Optional.of(timeReadMapping());
            case Types.TIMESTAMP:
                return Optional.of(timestampReadMapping());
            case Types.ARRAY:
                return Optional.of(arrayReadMapping());

        }
        return StandardReadMappings.jdbcTypeToPrestoType(typeHandle);
    }

    public static ReadMapping arrayReadMapping() {
        // return new ReadMapping(new
        // ArrayType(VarcharType.createUnboundedVarcharType()),
        // java.sql.ResultSet::getArray);
        return StandardReadMappings.varcharReadMapping(VarcharType.createUnboundedVarcharType());
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException {
        return new ClickhouseQueryBuilder(identifierQuote).buildSql(this, connection, split.getCatalogName(),
                split.getSchemaName(), split.getTableName(), columnHandles, split.getTupleDomain(),
                split.getAdditionalPredicate());
    }

    private static String singleQuote(String literal) {
        return "\'" + literal + "\'";
    }
}
