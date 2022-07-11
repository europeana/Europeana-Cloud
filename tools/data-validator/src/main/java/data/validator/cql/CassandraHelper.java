package data.validator.cql;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static data.validator.constants.Constants.*;

/**
 * Created by Tarek on 5/15/2017.
 */
public class CassandraHelper {

    public static BoundStatement prepareBoundStatementForMatchingTargetTable(CassandraConnectionProvider cassandraConnectionProvider, String targetTableName, List<String> primaryKeys) {
        String matchCountStatementCQL = CQLBuilder.getMatchCountStatementFromTargetTable(targetTableName, primaryKeys);
        PreparedStatement matchCountStatementCQLStatement = cassandraConnectionProvider.getSession().prepare(matchCountStatementCQL);
        return matchCountStatementCQLStatement.bind();
    }

    public static ResultSet getPrimaryKeysFromSourceTable(CassandraConnectionProvider cassandraConnectionProvider, String sourceTableName, List<String> primaryKeys) {
        String selectPrimaryKeysFromSourceTable = CQLBuilder.constructSelectPrimaryKeysFromSourceTable(sourceTableName, primaryKeys);
        PreparedStatement sourceSelectStatement = cassandraConnectionProvider.getSession().prepare(selectPrimaryKeysFromSourceTable);
        BoundStatement boundStatement = sourceSelectStatement.bind();
        return cassandraConnectionProvider.getSession().execute(boundStatement);
    }

    public static List<String> getPrimaryKeysNames(CassandraConnectionProvider cassandraConnectionProvider, String tableName, String selectColumnNames) {
        List<String> names = new LinkedList<>();
        PreparedStatement selectStatement = cassandraConnectionProvider.getSession().prepare(selectColumnNames);
        BoundStatement boundStatement = selectStatement.bind(cassandraConnectionProvider.getKeyspaceName(), tableName);
        ResultSet rs = cassandraConnectionProvider.getSession().execute(boundStatement);
        Iterator<Row> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (row.getString(COLUMN_INDEX_TYPE).equals(CLUSTERING_KEY_TYPE) || row.getString(COLUMN_INDEX_TYPE).equals(PARTITION_KEY_TYPE))
                names.add(row.getString(COLUMN_NAME_SELECTOR));
        }
        return names;
    }
}
