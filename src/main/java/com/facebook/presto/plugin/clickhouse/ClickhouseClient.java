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
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import ru.yandex.clickhouse.ClickHouseDriver;

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
                    //     String columnName = resultSet.getString("COLUMN_NAME");
                    //     boolean nullable = ResultSetMetaData.columnNullable == resultSet.getInt("NULLABLE");
                    //     // columns.add(
                    //     // new JdbcColumnHandle(connectorId, columnName, typeHandle,
                    //     // new com.facebook.presto.spi.type.ArrayType(
                    //     // VarcharType.createVarcharType(resultSet.getInt("COLUMN_SIZE"))),
                    //     // nullable));
                    //     typeHandle = new JdbcTypeHandle(Types.VARCHAR, resultSet.getInt("COLUMN_SIZE"),
                    //             resultSet.getInt("DECIMAL_DIGITS"));
                    //     columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle,
                    //             VarcharType.createUnboundedVarcharType(), nullable));
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

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle) {

        Optional<ReadMapping> s = StandardReadMappings.jdbcTypeToPrestoType(typeHandle);
        if (s.isPresent()) {
            return s;
        } else {

            switch (typeHandle.getJdbcType()) {
                case Types.ARRAY:

                    return Optional.of(arrayReadMapping());

            }
            return Optional.empty();
        }
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
