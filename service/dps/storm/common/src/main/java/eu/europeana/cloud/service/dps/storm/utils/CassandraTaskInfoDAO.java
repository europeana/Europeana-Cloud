package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;

import java.util.Date;
import java.util.Optional;

/**
 * The {@link eu.europeana.cloud.common.model.dps.TaskInfo} DAO
 *
 * @author akrystian
 */
public class CassandraTaskInfoDAO extends CassandraDAO {
    private TasksByStateDAO tasksByStateDAO;
    private PreparedStatement taskSearchStatement;
    private PreparedStatement taskInsertStatement;
    private PreparedStatement taskInsertUpdateStateStatement;
    private PreparedStatement updateExpectedSize;
    private PreparedStatement updateTask;
    private PreparedStatement endTask;
    private PreparedStatement updateProcessedFiles;
    private PreparedStatement killTask;
    private PreparedStatement setStatus;
    private PreparedStatement updateStatusExpectedSizeStatement;

    private static CassandraTaskInfoDAO instance = null;

    public static synchronized CassandraTaskInfoDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new CassandraTaskInfoDAO(cassandra);
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraTaskInfoDAO(CassandraConnectionProvider dbService) {
        super(dbService);
        tasksByStateDAO=new TasksByStateDAO(dbService);
    }

    @Override
    void prepareStatements() {
        taskSearchStatement = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        taskSearchStatement.setConsistencyLevel(dbService.getConsistencyLevel());
        updateTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.START_TIME + " = ? , " + CassandraTablesAndColumnsNames.INFO + " =? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateTask.setConsistencyLevel(dbService.getConsistencyLevel());
        updateProcessedFiles = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + " = ? , " + CassandraTablesAndColumnsNames.ERRORS + " = ? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateProcessedFiles.setConsistencyLevel(dbService.getConsistencyLevel());
        endTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + " = ? , " + CassandraTablesAndColumnsNames.ERRORS + " = ? , " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.FINISH_TIME + " = ? , " + CassandraTablesAndColumnsNames.INFO + " =? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        endTask.setConsistencyLevel(dbService.getConsistencyLevel());
        taskInsertStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE +
                "(" + CassandraTablesAndColumnsNames.BASIC_TASK_ID + ","
                + CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME + ","
                + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + ","
                + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + ","
                + CassandraTablesAndColumnsNames.STATE + ","
                + CassandraTablesAndColumnsNames.INFO + ","
                + CassandraTablesAndColumnsNames.SENT_TIME + ","
                + CassandraTablesAndColumnsNames.START_TIME + ","
                + CassandraTablesAndColumnsNames.FINISH_TIME + ","
                + CassandraTablesAndColumnsNames.ERRORS + ","
                + CassandraTablesAndColumnsNames.TASK_INFORMATIONS +
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?)");
        taskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        taskInsertUpdateStateStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE +
                "(" + CassandraTablesAndColumnsNames.BASIC_TASK_ID + ","
                + CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME + ","
                + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + ","
                + CassandraTablesAndColumnsNames.STATE + ","
                + CassandraTablesAndColumnsNames.INFO +
                ") VALUES (?,?,?,?,?)");
        taskInsertUpdateStateStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        killTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.INFO + " = ? , " + CassandraTablesAndColumnsNames.FINISH_TIME + " = ? " + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        killTask.setConsistencyLevel(dbService.getConsistencyLevel());

        updateExpectedSize = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + " = ?  WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateExpectedSize.setConsistencyLevel(dbService.getConsistencyLevel());

        setStatus = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.INFO + " =? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        setStatus.setConsistencyLevel(dbService.getConsistencyLevel());

        updateStatusExpectedSizeStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + " = ?  WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateStatusExpectedSizeStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    public TaskInfo searchById(long taskId)
            throws NoHostAvailableException, QueryExecutionException, TaskInfoDoesNotExistException {
        ResultSet rs = dbService.getSession().execute(taskSearchStatement.bind(taskId));
        if (!rs.iterator().hasNext()) {
            throw new TaskInfoDoesNotExistException();
        }
        Row row = rs.one();
        TaskInfo task = new TaskInfo(
                row.getLong(CassandraTablesAndColumnsNames.BASIC_TASK_ID),
                row.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME),
                TaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.STATE)),
                row.getString(CassandraTablesAndColumnsNames.INFO),
                row.getDate(CassandraTablesAndColumnsNames.SENT_TIME),
                row.getDate(CassandraTablesAndColumnsNames.START_TIME),
                row.getDate(CassandraTablesAndColumnsNames.FINISH_TIME)
        );
        task.setExpectedSize(row.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE));
        task.setProcessedElementCount(row.getInt(CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT));
        task.setTaskDefinition(row.getString("task_informations"));
        return task;
    }


    public void insert(long taskId, String topologyName, int expectedSize, String state, String info,String applicationIdentifier, String topicName) throws NoHostAvailableException, QueryExecutionException {
        tasksByStateDAO.insert(findTaskStatus(taskId),state,topologyName,taskId,applicationIdentifier,topicName);
        dbService.getSession().execute(taskInsertUpdateStateStatement.bind(taskId, topologyName, expectedSize, state, info));
    }

    public void insert(long taskId, String topologyName, int expectedSize, String state, String info, Date sentTime, String taskInformations, String applicationIdentifier, String topicName)
            throws NoHostAvailableException, QueryExecutionException {
        tasksByStateDAO.insert(findTaskStatus(taskId),state,topologyName,taskId,applicationIdentifier,topicName);
        dbService.getSession().execute(taskInsertStatement.bind(taskId, topologyName, expectedSize, 0, state, info, sentTime, null, null, 0, taskInformations));
    }

    public void insert(long taskId, String topologyName, int expectedSize, String state, String info, Date sentTime, String taskInformations)
            throws NoHostAvailableException, QueryExecutionException {
        insert(taskId, topologyName, expectedSize, state, info, sentTime, taskInformations, "","");
    }

    public void insert(long taskId, String topologyName, int expectedSize, int processedFilesCount, String state, String info, Date sentTime, Date startTime, Date finishTime, int errors, String taskInformations)
            throws NoHostAvailableException, QueryExecutionException {
        tasksByStateDAO.insert(findTaskStatus(taskId), state,topologyName,taskId,"","");
        dbService.getSession().execute(taskInsertStatement.bind(taskId, topologyName, expectedSize, processedFilesCount, state, info, sentTime, startTime, finishTime, errors, taskInformations));
    }

    public void updateTask(long taskId, String info, String state, Date startDate)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId,state);
        dbService.getSession().execute(updateTask.bind(String.valueOf(state), startDate, info, taskId));
    }

    public void setTaskCompletelyProcessed(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId,TaskState.PROCESSED.toString());
        dbService.getSession().execute(setStatus.bind(TaskState.PROCESSED.toString(), info, taskId));
    }

    public void setTaskDropped(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId,TaskState.DROPPED.toString());
        dbService.getSession().execute(killTask.bind(String.valueOf(TaskState.DROPPED), info, new Date(), taskId));
    }

    public void setUpdateExpectedSize(long taskId, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateExpectedSize.bind(expectedSize, taskId));
    }

    public void endTask(long taskId, int processeFilesCount, int errors, String info, String state, Date finishDate)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId, state);
        dbService.getSession().execute(endTask.bind(processeFilesCount, errors, String.valueOf(state), finishDate, info, taskId));
    }

    public void setUpdateProcessedFiles(long taskId, int processedFilesCount, int errors)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateProcessedFiles.bind(processedFilesCount, errors, taskId));
    }

    public boolean hasKillFlag(long taskId) throws TaskInfoDoesNotExistException {
        String state = getTaskStatus(taskId);
        if (state.equals(String.valueOf(TaskState.DROPPED)))
            return true;
        return false;
    }

    private String getTaskStatus(long taskId) throws TaskInfoDoesNotExistException {
        return findTaskStatus(taskId)
                .orElseThrow(TaskInfoDoesNotExistException::new);
    }

    private Optional<String> findTaskStatus(long taskId) {
        return findTaskRow(taskId)
                .map(row -> row.getString(CassandraTablesAndColumnsNames.STATE));
    }

    private Optional<Row> findTaskRow(long taskId)   {
        ResultSet rs = dbService.getSession().execute(taskSearchStatement.bind(taskId));
        return Optional.ofNullable(rs.one());
    }


    public void updateStatusExpectedSize(long taskId, String state, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        updateTasksByTaskStateTable(taskId,state);
        dbService.getSession().execute(updateStatusExpectedSizeStatement.bind(String.valueOf(state), expectedSize, taskId));
    }

    private void updateTasksByTaskStateTable(long taskId, String newState) {
        Optional<Row> oldTask = findTaskRow(taskId);
        if(oldTask.isPresent()) {
            String topologyName = oldTask.get().getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME);
            String oldState = oldTask.get().getString(CassandraTablesAndColumnsNames.STATE);
            tasksByStateDAO.updateTask(topologyName, taskId, oldState, newState);
        }
    }

}
