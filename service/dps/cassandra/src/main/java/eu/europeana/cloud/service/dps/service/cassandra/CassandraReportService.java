package eu.europeana.cloud.service.dps.service.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.*;
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

    private static final int FETCH_SIZE = 100;
    private static final int FETCH_ONE = 1;
    private PreparedStatement selectErrorsStatement;
    private PreparedStatement selectErrorStatement;
    private PreparedStatement selectErrorCounterStatement;

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
        prepareStatements();
    }

    private void prepareStatements() {
        selectErrorsStatement = cassandra.getSession().prepare("SELECT * FROM " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ?");
        selectErrorsStatement.setConsistencyLevel(cassandra.getConsistencyLevel());

        selectErrorStatement = cassandra.getSession().prepare("SELECT * FROM " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + " = ? LIMIT ?");
        selectErrorStatement.setConsistencyLevel(cassandra.getConsistencyLevel());

        selectErrorCounterStatement = cassandra.getSession().prepare("SELECT * FROM " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE + " = ?");
        selectErrorCounterStatement.setConsistencyLevel(cassandra.getConsistencyLevel());
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
                    basicInfo.getInt(CassandraTablesAndColumnsNames.ERRORS),
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

    /**
     * Retrieve all errors that occurred for the given task
     *
     * @param task task identifier
     * @return task error info object
     * @throws AccessDeniedOrObjectDoesNotExistException
     */
    @Override
    public TaskErrorsInfo getGeneralTaskErrorReport(String task) throws AccessDeniedOrObjectDoesNotExistException {
        long taskId = Long.valueOf(task);
        ResultSet rs = cassandra.getSession().execute(selectErrorsStatement.bind(taskId));
        if (!rs.iterator().hasNext()) {
            throw new AccessDeniedOrObjectDoesNotExistException("The task with the provided id doesn't exist!");
        }

        List<TaskErrorInfo> errors = new ArrayList<>();
        Map<String, String> errorMessages = new HashMap<>();

        while (rs.iterator().hasNext()) {
            Row row = rs.one();

            String errorType = row.getUUID(CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE).toString();
            String errorMessage = getErrorMessage(taskId, errorMessages, errorType);
            errors.add(new TaskErrorInfo(errorType,
                    errorMessage,
                    (int) row.getLong(CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER)));
        }
        return new TaskErrorsInfo(taskId, errors);
    }

    /**
     * Retrieve the specific error message. First it tries to retrieve it from the map that caches the messages
     * by their error type. If not present it fetches one row from the table.
     *
     * @param taskId task identifier
     * @param errorMessages map of error messages
     * @param errorType error type
     * @return error message
     * @throws AccessDeniedOrObjectDoesNotExistException
     */
    private String getErrorMessage(long taskId, Map<String, String> errorMessages, String errorType) throws AccessDeniedOrObjectDoesNotExistException {
        String message = errorMessages.get(errorType);
        if (message == null) {
            ResultSet rs = cassandra.getSession().execute(selectErrorStatement.bind(taskId, UUID.fromString(errorType), FETCH_ONE));
            if (!rs.iterator().hasNext()) {
                throw new AccessDeniedOrObjectDoesNotExistException("The task with the provided id doesn't exist!");
            }
            message = rs.one().getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE);
            errorMessages.put(errorType, message);
        }
        return message;
    }


    /**
     * Retrieve sample of identifiers (max {@value #FETCH_SIZE}) for the given error type
     *
     * @param task task identifier
     * @param errorType type of error
     *
     * @return task error info objects with sample identifiers
     */
    @Override
    public TaskErrorsInfo getSpecificTaskErrorReport(String task, String errorType) throws AccessDeniedOrObjectDoesNotExistException {
        long taskId = Long.valueOf(task);
        ResultSet rs = cassandra.getSession().execute(selectErrorStatement.bind(taskId, UUID.fromString(errorType), FETCH_SIZE));
        if (!rs.iterator().hasNext()) {
            throw new AccessDeniedOrObjectDoesNotExistException("The task with the provided id doesn't exist!");
        }

        List<String> identifiers = new ArrayList<>();
        String message = null;

        while (rs.iterator().hasNext()) {
            Row row = rs.one();
            identifiers.add(row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_RESOURCE));
            if (message == null) {
                message = row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE);
            }
        }
        TaskErrorInfo taskErrorInfo = getTaskErrorInfo(taskId, errorType);
        taskErrorInfo.setIdentifiers(identifiers);
        taskErrorInfo.setMessage(message);
        return new TaskErrorsInfo(taskId, Arrays.asList(taskErrorInfo));
    }

    private TaskErrorInfo getTaskErrorInfo(long taskId, String errorType) throws AccessDeniedOrObjectDoesNotExistException {
        ResultSet rs = cassandra.getSession().execute(selectErrorCounterStatement.bind(taskId, UUID.fromString(errorType)));
        if (!rs.iterator().hasNext()) {
            throw new AccessDeniedOrObjectDoesNotExistException("The task with the provided id doesn't exist!");
        }

        TaskErrorInfo taskErrorInfo = new TaskErrorInfo();
        taskErrorInfo.setErrorType(errorType);

        Row row = rs.one();
        taskErrorInfo.setOccurrences((int) row.getLong(CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER));

        return taskErrorInfo;
    }
}
