package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.ResourceProgress;
import eu.europeana.cloud.common.model.dps.States;

/**
 * Created by Tarek on 8/7/2018.
 */
public class CassandraResourceProgressDAO extends CassandraDAO {
    private PreparedStatement insertResourceStatement;
    private PreparedStatement getResourceStatement;
    private PreparedStatement updateResourceStatement;

    private static CassandraResourceProgressDAO instance = null;

    public static synchronized CassandraResourceProgressDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new CassandraResourceProgressDAO(cassandra);
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    private CassandraResourceProgressDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        insertResourceStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.RESOURCE_PROGRESS + "("
                + CassandraTablesAndColumnsNames.BASIC_TASK_ID
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE
                + "," + CassandraTablesAndColumnsNames.STATE
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE
                + ") VALUES (?,?,?,?)");
        insertResourceStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        getResourceStatement = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.RESOURCE_PROGRESS + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ? AND " +
                        CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE + " = ? ");
        getResourceStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateResourceStatement = dbService.getSession().prepare(
                "update " + CassandraTablesAndColumnsNames.RESOURCE_PROGRESS + " set " + CassandraTablesAndColumnsNames.STATE + " = ? where " +
                        CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ? AND " +
                        CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE + " = ? ");
        updateResourceStatement.setConsistencyLevel(dbService.getConsistencyLevel());

    }

    public void insert(long taskId, String resource, String state, String resultResource)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(insertResourceStatement.bind(taskId, resource, state, resultResource));
    }

    public void updateResourceProgress(long taskId, String resource, String state)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateResourceStatement.bind(state, taskId, resource));
    }

    public ResourceProgress searchForResourceProgress(long taskId, String resource)
            throws NoHostAvailableException, QueryExecutionException {
        ResultSet rs = dbService.getSession().execute(getResourceStatement.bind(taskId, resource));
        if (!rs.iterator().hasNext())
            return null;
        Row row = rs.one();

        return new ResourceProgress(row.getLong(CassandraTablesAndColumnsNames.BASIC_TASK_ID),
                row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE),
                States.valueOf(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE)),
                row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE)
        );
    }
}
