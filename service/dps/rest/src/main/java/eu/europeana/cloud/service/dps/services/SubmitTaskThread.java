package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.converters.DpsTaskToHarvestConverter;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.structs.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.util.List;

@Component
@Scope("prototype")
@Repository
public class SubmitTaskThread extends Thread {
    private final static Logger LOGGER = LoggerFactory.getLogger(SubmitTaskThread.class);

    private final static int UNKNOWN_EXPECTED_SIZE = -1;

    @Autowired
    private HarvestsExecutor harvestsExecutor;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private FilesCounterFactory filesCounterFactory;

    @Autowired
    private PermissionManager permissionManager;

    @Autowired
    private String applicationIdentifier;

    @Autowired
    private KafkaTopicSelector kafkaTopicSelector;

    @Autowired
    private TaskExecutionSubmitService submitService;

    private SubmitTaskParameters parameters;

    public SubmitTaskThread(SubmitTaskParameters parameters) {
        super("submit-task-thread");
        this.parameters = parameters;
    }

    @Override
    public void run() {
        try {
            ResponseEntity<Void> response = ResponseEntity.created(parameters.getResponsURI()).build();
            insertTask(0, TaskState.PROCESSING_BY_REST_APPLICATION.toString(), "The task is in a pending mode, it is being processed before submission", "");
            permissionManager.grantPermissionsForTask(String.valueOf(parameters.getTask().getTaskId()));
            parameters.getResponseFuture().complete(response);
            int expectedCount = getFilesCountInsideTask();
            LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);
            if (expectedCount == 0) {
                insertTask(expectedCount, TaskState.DROPPED.toString(), "The task doesn't include any records", "");
            } else {
                if (parameters.getTopologyName().equals(TopologiesNames.OAI_TOPOLOGY)) {
                    String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTopologyName());
                    insertTask(expectedCount, TaskState.PROCESSING_BY_REST_APPLICATION.toString(), "Task submitted successfully and processed by REST app", preferredTopicName);
                    List<Harvest> harvestsToByExecuted = new DpsTaskToHarvestConverter().from(parameters.getTask());

                    HarvestResult harvesterResult;
                    if (!parameters.isRestart()) {
                        harvesterResult = harvestsExecutor.execute(parameters.getTopologyName(), harvestsToByExecuted, parameters.getTask(), preferredTopicName);
                    } else {
                        harvesterResult = harvestsExecutor.executeForRestart(parameters.getTopologyName(), harvestsToByExecuted, parameters.getTask(), preferredTopicName);
                    }
                    updateTaskStatus(harvesterResult);
                } else {
                    parameters.getTask().addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, parameters.getAuthorizationHeader());
                    submitService.submitTask(parameters.getTask(), parameters.getTopologyName());
                    insertTask(expectedCount, TaskState.SENT.toString(), "", "");
                }
                LOGGER.info("Task submitted successfully to Kafka");
            }
        } catch (TaskSubmissionException e) {
            LOGGER.error("Task submission failed: {}", e.getMessage(), e);
            insertTask(0, TaskState.DROPPED.toString(), e.getMessage(), "");
            ResponseEntity<Void> response = ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            parameters.getResponseFuture().complete(response);
        } catch (Exception e) {
            String fullStacktrace = ExceptionUtils.getStackTrace(e);
            LOGGER.error("Task submission failed: {}", fullStacktrace);
            insertTask(0, TaskState.DROPPED.toString(), fullStacktrace, "");
            ResponseEntity<Void> response = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            parameters.getResponseFuture().complete(response);
        }
    }

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
    private void insertTask(int expectedSize, String state, String info, String topicName) {
        taskInfoDAO.insert(parameters.getTask().getTaskId(), parameters.getTopologyName(), expectedSize, state, info,
                parameters.getSentTime(), parameters.getTaskJSON());

        tasksByStateDAO.delete(TaskState.PROCESSING_BY_REST_APPLICATION.toString(), parameters.getTopologyName(),
                parameters.getTask().getTaskId());

        tasksByStateDAO.insert(state, parameters.getTopologyName(), parameters.getTask().getTaskId(),
                applicationIdentifier, topicName);
    }


    private void updateTaskStatus(HarvestResult harvesterResult) {
        if (harvesterResult.getTaskState() != TaskState.DROPPED && harvesterResult.getResultCounter() == 0) {
            LOGGER.info("Task dropped. No data harvested");
            taskInfoDAO.dropTask(parameters.getTask().getTaskId(),
                    "The task with the submitted parameters is empty", TaskState.DROPPED.toString());
        } else {
            LOGGER.info("Updating task {} expected size to: {}", parameters.getTask().getTaskId(), harvesterResult.getResultCounter());
            taskInfoDAO.updateStatusExpectedSize(parameters.getTask().getTaskId(),
                    harvesterResult.getTaskState().toString(), harvesterResult.getResultCounter());
        }
    }

    /**
     * @return The number of files inside the task.
     */
    private int getFilesCountInsideTask() throws TaskSubmissionException {
        if (parameters.getTopologyName().equals(TopologiesNames.HTTP_TOPOLOGY)) {
            return UNKNOWN_EXPECTED_SIZE;
        }
        String taskType = getTaskType();
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(taskType);
        return filesCounter.getFilesCount(parameters.getTask());
    }

    //get TaskType
    private String getTaskType() {
        //TODO sholud be done in more error prone way
        final InputDataType first = parameters.getTask().getInputData().keySet().iterator().next();
        return first.name();
    }
}

