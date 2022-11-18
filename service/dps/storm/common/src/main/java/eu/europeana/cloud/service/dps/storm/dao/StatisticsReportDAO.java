package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class StatisticsReportDAO extends CassandraDAO {
    private static StatisticsReportDAO instance = null;

    private final Gson gson = new Gson();

    private PreparedStatement getStatisticsReportStatement;
    private PreparedStatement removeStatisticsReportStatement;
    private PreparedStatement storeStatisticsReportStatement;
    private PreparedStatement checkStatisticsReportStatement;

    /**
     * @param dbService The service exposing the connection and session
     */
    public StatisticsReportDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public StatisticsReportDAO() {
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
    }

    public static synchronized StatisticsReportDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new StatisticsReportDAO(cassandra));
        }
        return instance;
    }



    @Override
    protected void prepareStatements() {

        storeStatisticsReportStatement = dbService.getSession().prepare(
                "INSERT INTO " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE
                        + " ("
                        + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + ","
                        + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_REPORT_DATA
                        + ") VALUES (?,textasblob(?))"
        );

        checkStatisticsReportStatement = dbService.getSession().prepare(
                "SELECT " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID
                        + " FROM " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + " = ?"
        );

        getStatisticsReportStatement = dbService.getSession().prepare(
                "SELECT blobastext(" + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_REPORT_DATA + ")"
                        + " FROM " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + " = ?"
        );

        removeStatisticsReportStatement = dbService.getSession().prepare(
                "DELETE  FROM " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + " = ?"
        );
    }

    public void storeReport(long taskId, StatisticsReport report) {
        String reportSerialized = gson.toJson(report);
        if (reportSerialized != null) {
            BoundStatement bs = storeStatisticsReportStatement.bind(taskId, reportSerialized);
            dbService.getSession().execute(bs);
        }
    }

    /**
     * Check whether report for the specific task has already been stored.
     *
     * @param taskId task identifier
     * @return true when a row for the given task identifier is in the table
     */
    public boolean isReportStored(long taskId) {
        BoundStatement bs = checkStatisticsReportStatement.bind(taskId);
        ResultSet rs = dbService.getSession().execute(bs);

        return rs.iterator().hasNext();
    }

    /**
     * Return statistics report from the database. When not present null will be returned.
     *
     * @param taskId task identifier
     * @return statistics report object
     */
    public StatisticsReport getStatisticsReport(long taskId) {
        BoundStatement bs = getStatisticsReportStatement.bind(taskId);
        ResultSet rs = dbService.getSession().execute(bs);

        if (rs.iterator().hasNext()) {
            Row row = rs.one();

            String report = row.getString(0);
            return gson.fromJson(report, StatisticsReport.class);
        }
        return null;
    }

    public void removeStatisticsReport(long taskId) {
        dbService.getSession().execute(removeStatisticsReportStatement.bind(taskId));
    }

}