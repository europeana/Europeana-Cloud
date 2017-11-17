package eu.europeana.cloud.service.dps.service.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;

import java.util.*;

/**
 * Report service powered by Cassandra.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class CassandraReportService implements TaskExecutionReportService {
    CassandraConnectionProvider cassandra;

    /**
     * Constructor of Cassandra report service.
     *
     * @param hosts        Cassandra hosts separated by comma (e.g. localhost,192.168.47.129)
     * @param port         Cassandra port
     * @param keyspaceName Cassandra keyspace name
     * @param userName     Cassandra username
     * @param password     Cassandra password
     */
    public CassandraReportService(String hosts, int port, String keyspaceName, String userName, String password) {
        cassandra = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }

    @Override
    public TaskInfo getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException {
        long taskIdValue = Long.valueOf(taskId);
        Statement selectFromBasicInfo = QueryBuilder.select().all()
                .from(CassandraTablesAndColumnsNames.BASIC_INFO_TABLE)
                .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.BASIC_TASK_ID, taskIdValue));

        Row basicInfo = cassandra.getSession().execute(selectFromBasicInfo).one();
        if (basicInfo != null) {
            TaskInfo taskInfo = new TaskInfo(taskIdValue,
                    basicInfo.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME),
                    TaskState.valueOf(basicInfo.getString(CassandraTablesAndColumnsNames.STATE)),
                    basicInfo.getString(CassandraTablesAndColumnsNames.INFO),
                    basicInfo.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE),
                    basicInfo.getInt(CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT),
                    basicInfo.getDate(CassandraTablesAndColumnsNames.SENT_TIME),
                    basicInfo.getDate(CassandraTablesAndColumnsNames.START_TIME),
                    basicInfo.getDate(CassandraTablesAndColumnsNames.FINISH_TIME));
            return taskInfo;
        }
        throw new AccessDeniedOrObjectDoesNotExistException("The task with the provided id doesn't exist!");
    }


    @Override
    public List<SubTaskInfo> getDetailedTaskReportBetweenChunks(String taskId, int from, int to) {
        Statement selectFromNotification = QueryBuilder.select()
                .from(CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE)
                .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, Long.valueOf(taskId))).and(QueryBuilder.gte(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM, from)).and(QueryBuilder.lte(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM, to));

        ResultSet detailedTaskReportResultSet = cassandra.getSession().execute(selectFromNotification);

        return convertDetailedTaskReportToListOfSubTaskInfo(detailedTaskReportResultSet);
    }


    @Override
    public void incrTaskProgress(String taskId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private List<SubTaskInfo> convertDetailedTaskReportToListOfSubTaskInfo(ResultSet data) {

        List<SubTaskInfo> subTaskInfoList = new ArrayList<>();

        for (Row row : data) {
            SubTaskInfo subTaskInfo = new SubTaskInfo(row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM),
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE),
                    States.valueOf(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE)),
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT),
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS),
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE));
            subTaskInfoList.add(subTaskInfo);
        }
        return subTaskInfoList;
    }
}
