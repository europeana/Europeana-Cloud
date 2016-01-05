package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.service.cassandra.CassandraTablesAndColumnsNames;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

import java.util.ArrayList;
import java.util.List;

public class CassandraTaskInfoDAO {

    private String hostList;
    private String keyspaceName;
    private String port;
    private CassandraConnectionProvider dbService;
    private PreparedStatement subtaskSearchStatement;
    private PreparedStatement taskSearchStatement;
    private PreparedStatement taskInsertStatement;

    /**
     * The TaskiInfo Dao
     *
     * @param dbService The service exposing the connection and session
     */
    public CassandraTaskInfoDAO(CassandraConnectionProvider dbService) {
        this.dbService = dbService;
        this.hostList = dbService.getHosts();
        this.port = dbService.getPort();
        this.keyspaceName = dbService.getKeyspaceName();
        prepareStatements();
    }

    public String getHostList() {
        return hostList;
    }

    public String getKeyspace() {
        return keyspaceName;
    }

    public String getPort() {
        return this.port;
    }

    private void prepareStatements() {
        taskSearchStatement = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        subtaskSearchStatement = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE + " WHERE " + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID + " = ?");
        taskInsertStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE +
                "(" + CassandraTablesAndColumnsNames.BASIC_TASK_ID + "," + CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME + "," + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE +
                ") VALUES (?,?,?)");
    }

    public List<TaskInfo> searchById(String... args) throws DatabaseConnectionException, TaskInfoDoesNotExistException {
        try {
            ResultSet rs = dbService.getSession().execute(taskSearchStatement.bind(new Long(args[0])));
            if (!rs.iterator().hasNext()) {

                throw new TaskInfoDoesNotExistException(new IdentifierErrorInfo(
                        IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                        IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(args[0])));
            }
            List<TaskInfo> result = new ArrayList<>();
            for (Row row : rs.all()) {
                TaskInfo task = new TaskInfo(row.getLong(CassandraTablesAndColumnsNames.BASIC_TASK_ID), row.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME));
                task.setContainsElements(row.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE));
                result.add(task);
            }
            return result;
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(hostList, port, e.getMessage())));
        }
    }

    public List<TaskInfo> searchByIdWithSubtasks(String... args) throws DatabaseConnectionException, TaskInfoDoesNotExistException {
        List<TaskInfo> result = searchById(args);
        try {
            for (TaskInfo taskInfo : result) {
                ResultSet rs = dbService.getSession().execute(subtaskSearchStatement.bind(new Long(args[0])));
                if (!rs.iterator().hasNext()) {
                    throw new RuntimeException();
                }
                for (Row row : rs.all()) {
                    taskInfo.addSubtask(
                            new SubTaskInfo(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE),
                                    States.valueOf(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE)),
                                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT),
                                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS)
                            ));
                }
            }
            return result;
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(hostList, port, e.getMessage())));
        }
    }

    public void insert(String... args) throws DatabaseConnectionException {
        try {
            dbService.getSession().execute(taskInsertStatement.bind(new Long(args[0]), args[1], args[2]));
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(hostList, port, e.getMessage())));
        }
    }
}
