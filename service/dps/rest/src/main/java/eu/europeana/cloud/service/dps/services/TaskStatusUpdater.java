package eu.europeana.cloud.service.dps.services;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

//TODO: needs further refactorization (i.e. methods names)
@Service
public class TaskStatusUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusUpdater.class);

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private String applicationIdentifier;

    /**
     * Inserts/update given task in db. Two tables are modified {@link CassandraTablesAndColumnsNames#BASIC_INFO_TABLE}
     * and {@link CassandraTablesAndColumnsNames#TASKS_BY_STATE_TABLE}<br/>
     * NOTE: Operation is not in transaction! So on table can be modified but second one not
     * Parameters corresponding to names of column in table(s)
     *
     * @param expectedSize
     * @param state
     * @param info
     */
    public void insertTask(long taskId, String topologyName, int expectedSize, String state, String info, String topicName) {
        taskInfoDAO.insert(taskId, topologyName, expectedSize, state, info);
        tasksByStateDAO.delete(TaskState.PROCESSING_BY_REST_APPLICATION.toString(), topologyName, taskId);
        tasksByStateDAO.insert(state, topologyName, taskId, applicationIdentifier, topicName);
    }

    public void dropTask(long taskId, String info) {
        taskInfoDAO.dropTask(taskId, info,
                TaskState.DROPPED.toString());
    }

    public void updateStatusExpectedSize(long taskId, String state, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        LOGGER.info("Updating task {} expected size to: {}", taskId, expectedSize);
        taskInfoDAO.updateStatusExpectedSize(taskId, state, expectedSize);
    }
}
