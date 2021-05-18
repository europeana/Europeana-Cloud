package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.GeneralStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

import java.util.List;
import java.util.stream.Collectors;

public class CassandraGeneralStatisticsDAO extends CassandraDAO {
    private static CassandraGeneralStatisticsDAO instance = null;

    private PreparedStatement updateStatement;
    private PreparedStatement removeGeneralStatisticsStatement;
    private PreparedStatement searchGeneralStatistcsByTaskIdStatement;
    private PreparedStatement searchByParentStatement;
    private PreparedStatement searchByNodeStatement;


    public static synchronized CassandraGeneralStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new CassandraGeneralStatisticsDAO(cassandra));
        }
        return instance;
    }

    public CassandraGeneralStatisticsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public CassandraGeneralStatisticsDAO() {
    }

    @Override
    void prepareStatements() {
        updateStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " SET " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE + " = " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE + " + 1 " +
                "WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH + " = ?");
        updateStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        removeGeneralStatisticsStatement = dbService.getSession().prepare("DELETE " +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ?");
        removeGeneralStatisticsStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchGeneralStatistcsByTaskIdStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ?");
        searchGeneralStatistcsByTaskIdStatement.setConsistencyLevel(dbService.getConsistencyLevel());


        searchByParentStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH + " = ?");
        searchByParentStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchByNodeStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH + " = ?");
        searchByNodeStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    /**
     * It will update the counter for the specified node in general statistics table
     *
     * @param taskId         task identifier
     * @param nodeStatistics node statistics object with all the necessary information
     */
    public void updateGeneralStatistics(long taskId, NodeStatistics nodeStatistics) {
        dbService.getSession().execute(updateStatement.bind(taskId, nodeStatistics.getParentXpath(), nodeStatistics.getXpath()));
    }

    public void removeGeneralStatistics(long taskId) {
        dbService.getSession().execute(removeGeneralStatisticsStatement.bind(taskId));
    }

    public List<GeneralStatistics> searchGeneralStatistics(long taskId) {
        return dbService.getSession().execute(searchGeneralStatistcsByTaskIdStatement.bind(taskId)).all().stream().map(this::createGeneralStatistics).collect(Collectors.toList());
    }

    private GeneralStatistics createGeneralStatistics(Row row) {
        return GeneralStatistics.builder()
                .parentXpath(row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH))
                .nodeXpath(row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH))
                .occurrence(row.getLong(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE))
                .build();
    }
}