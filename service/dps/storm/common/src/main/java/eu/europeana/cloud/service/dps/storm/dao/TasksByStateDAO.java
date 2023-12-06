package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASKS_BY_STATE_APP_ID_COL_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASKS_BY_STATE_START_TIME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASKS_BY_STATE_STATE_COL_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASKS_BY_STATE_TABLE;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASKS_BY_STATE_TASK_ID_COL_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASKS_BY_STATE_TOPIC_NAME_COL_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASKS_BY_STATE_TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_INFO_STATE;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.EnumUtils;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class TasksByStateDAO extends CassandraDAO {

  private static TasksByStateDAO instance;

  private PreparedStatement insertStatement;
  private PreparedStatement deleteStatement;
  private PreparedStatement findTasksByStateStatement;
  private PreparedStatement findTasksByStateAndTopologyStatement;
  private PreparedStatement findTaskByStateAndTopologyStatement;
  private PreparedStatement findTaskByStateStatement;
  private PreparedStatement findTaskStatement;

  public TasksByStateDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public TasksByStateDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  public static synchronized TasksByStateDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new TasksByStateDAO(cassandra));
    }
    return instance;
  }


  @Override
  protected void prepareStatements() {
    insertStatement = dbService.getSession().prepare("INSERT INTO " + TASKS_BY_STATE_TABLE
        + "("
        + TASKS_BY_STATE_STATE_COL_NAME + ","
        + TASKS_BY_STATE_TOPOLOGY_NAME + ","
        + TASKS_BY_STATE_TASK_ID_COL_NAME + ","
        + TASKS_BY_STATE_APP_ID_COL_NAME + ","
        + TASKS_BY_STATE_TOPIC_NAME_COL_NAME + ","
        + TASKS_BY_STATE_START_TIME
        + ") VALUES (?,?,?,?,?,?)");

    deleteStatement = dbService.getSession().prepare("DELETE FROM " + TASKS_BY_STATE_TABLE
        + " WHERE " + TASK_INFO_STATE + " = ?"
        + " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?"
        + " AND " + TASKS_BY_STATE_TASK_ID_COL_NAME + " = ?"
    );

    findTaskStatement = dbService.getSession().prepare(
        "SELECT *"
            + " FROM " + TASKS_BY_STATE_TABLE
            + " WHERE " + TASK_INFO_STATE + " = ?"
            + " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?"
            + " AND " + TASKS_BY_STATE_TASK_ID_COL_NAME + " = ?"
    );

    findTasksByStateStatement = dbService.getSession().prepare(
        "SELECT *"
            + " FROM " + TASKS_BY_STATE_TABLE
            + " WHERE " + TASK_INFO_STATE
            + " IN ?"
    );

    findTasksByStateAndTopologyStatement = dbService.getSession().prepare(
        "SELECT * "
            + " FROM " + TASKS_BY_STATE_TABLE
            + " WHERE " + TASK_INFO_STATE + " IN ?"
            + " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?"
    );

    findTaskByStateStatement = dbService.getSession().prepare(
        String.format("select * from %s where %s in ? limit 1", TASKS_BY_STATE_TABLE, TASK_INFO_STATE)
    );

    findTaskByStateAndTopologyStatement = dbService.getSession().prepare(
        "SELECT * "
            + " FROM " + TASKS_BY_STATE_TABLE
            + " WHERE " + TASK_INFO_STATE + " IN ?"
            + " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?"
            + " LIMIT 1");

  }

  public void insert(TaskState state, String topologyName, long taskId, String applicationId, String topicName, Date startTime) {
    dbService.getSession().execute(insertStatement(state, topologyName, taskId, applicationId, topicName, startTime));
  }

  public BoundStatement insertStatement(TaskState state, String topologyName, long taskId, String applicationId, String topicName,
      Date startTime) {
    return insertStatement.bind(state.toString(), topologyName, taskId, applicationId, topicName, startTime);
  }

  public void delete(TaskState state, String topologyName, long taskId) {
    dbService.getSession().execute(deleteStatement(state, topologyName, taskId));
  }

  public BoundStatement deleteStatement(TaskState state, String topologyName, long taskId) {
    return deleteStatement.bind(state.toString(), topologyName, taskId);
  }

  public Optional<TaskByTaskState> findTask(TaskState state, String topologyName, long taskId) {
    var rs = dbService.getSession().execute(
        findTaskStatement.bind(state.toString(), topologyName, taskId)
    );
    return Optional.ofNullable(rs.one()).map(this::createTaskByTaskState);
  }

  public List<TaskByTaskState> findTasksByState(List<TaskState> taskStates) {
    var rs = dbService.getSession().execute(
        findTasksByStateStatement.bind(taskStates.stream().map(Enum::toString).toList()
        ));
    return rs.all().stream().map(this::createTaskByTaskState).toList();
  }

  public List<TaskByTaskState> findTasksByStateAndTopology(List<TaskState> taskStates, String topologyName) {
    var rs = dbService.getSession().execute(
        findTasksByStateAndTopologyStatement.bind(
            taskStates.stream().map(Enum::toString).toList(),
            topologyName
        )
    );
    return rs.all().stream().map(this::createTaskByTaskState).toList();
  }

  public Optional<TaskByTaskState> findTaskByState(List<TaskState> taskStates) {
    var rs = dbService.getSession().execute(
        findTaskByStateStatement.bind(
            taskStates.stream().map(Enum::toString).toList()
        )
    );
    return Optional.ofNullable(rs.one()).map(this::createTaskByTaskState);
  }

  public Optional<TaskByTaskState> findTaskByStateAndTopology(List<TaskState> taskStates, String topologyName) {
    var rs = dbService.getSession().execute(
        findTaskByStateAndTopologyStatement.bind(
            taskStates.stream().map(Enum::toString).toList(),
            topologyName
        )
    );
    return Optional.ofNullable(rs.one()).map(this::createTaskByTaskState);
  }


  private TaskByTaskState createTaskByTaskState(Row row) {
    return TaskByTaskState.builder()
                          .state(EnumUtils.getEnum(TaskState.class, row.getString(TASKS_BY_STATE_STATE_COL_NAME)))
                          .topologyName(row.getString(TASKS_BY_STATE_TOPOLOGY_NAME))
                          .id(row.getLong(TASKS_BY_STATE_TASK_ID_COL_NAME))
                          .applicationId(row.getString(TASKS_BY_STATE_APP_ID_COL_NAME))
                          .startTime(row.getTimestamp(TASKS_BY_STATE_START_TIME))
                          .topicName(row.getString(TASKS_BY_STATE_TOPIC_NAME_COL_NAME))
                          .build();
  }
}
