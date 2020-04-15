package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.converters.DpsTaskToHarvestConverter;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReader;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSTaskSubmiter;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.structs.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SubmitTaskService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubmitTaskService.class);

    private static final int UNKNOWN_EXPECTED_SIZE = -1;

    @Autowired
    private HarvestsExecutor harvestsExecutor;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private FilesCounterFactory filesCounterFactory;

    @Autowired
    private String applicationIdentifier;

    @Autowired
    private KafkaTopicSelector kafkaTopicSelector;

    @Autowired
    private TaskExecutionSubmitService submitService;

    @Autowired
    private CassandraTaskErrorsDAO taskErrorDAO;

    @Autowired
    private TaskStatusChecker taskStatusChecker;

    @Autowired
    private RecordExecutionSubmitService recordSubmitService;

    @Value("${/dps/mcsLocation}")
    private String mcsClientURL;

    @Async
    public void submitTask(SubmitTaskParameters parameters) {
        try {
            int expectedCount = getFilesCountInsideTask(parameters.getTask(), parameters.getTopologyName());
            LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);
            if (expectedCount == 0) {
                insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),
                        expectedCount, TaskState.DROPPED.toString(), "The task doesn't include any records", "");
            } else {
                if (!parameters.getTopologyName().equals(TopologiesNames.HTTP_TOPOLOGY)) {
                    String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTopologyName());
                    insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),
                            expectedCount, TaskState.PROCESSING_BY_REST_APPLICATION.toString(), "Task submitted successfully and processed by REST app", preferredTopicName);

                    if (parameters.getTopologyName().equals(TopologiesNames.OAI_TOPOLOGY)) {
                        List<Harvest> harvestsToByExecuted = new DpsTaskToHarvestConverter().from(parameters.getTask());

                        HarvestResult harvesterResult;
                        if (!parameters.isRestart()) {
                            harvesterResult = harvestsExecutor.execute(parameters.getTopologyName(), harvestsToByExecuted, parameters.getTask(), preferredTopicName);
                        } else {
                            harvesterResult = harvestsExecutor.executeForRestart(parameters.getTopologyName(), harvestsToByExecuted, parameters.getTask(), preferredTopicName);
                        }
                        updateTaskStatus(parameters.getTask().getTaskId(), harvesterResult);
                    } else {
                        parameters.getTask().addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, parameters.getAuthorizationHeader());
                        createMCSReader(parameters.getTopologyName(), parameters.getTask(), preferredTopicName).execute();
                        insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),
                                expectedCount, TaskState.SENT.toString(), "", "");
                    }

                } else { // HTTP_TOPOLOGY only
                    parameters.getTask().addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, parameters.getAuthorizationHeader());
                    submitService.submitTask(parameters.getTask(), parameters.getTopologyName());
                    insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),
                            expectedCount, TaskState.SENT.toString(), "", "");
                }
                LOGGER.info("Task {} submitted successfully to Kafka", parameters.getTask().getTaskId());
            }
        } catch (TaskSubmissionException e) {
            LOGGER.error("Task submission failed: {}", e.getMessage(), e);
            insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(), 0,
                    TaskState.DROPPED.toString(), e.getMessage(), "");
        } catch (Exception e) {
            String fullStacktrace = ExceptionUtils.getStackTrace(e);
            LOGGER.error("Task submission failed: {}", fullStacktrace);
            insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),0,
                    TaskState.DROPPED.toString(), fullStacktrace, "");
        }
    }

    private MCSTaskSubmiter createMCSReader(String topologyName, DpsTask task, String topicName){
        String authorizationHeader = task.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        MCSReader reader=new MCSReader(mcsClientURL,authorizationHeader);
        return new MCSTaskSubmiter(taskErrorDAO,taskStatusChecker,taskInfoDAO,recordSubmitService,topologyName,task,topicName,reader);
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
    private void insertTask(long taskId, String topologyName, int expectedSize, String state, String info, String topicName) {
        taskInfoDAO.insert(taskId, topologyName, expectedSize, state, info);
        tasksByStateDAO.delete(TaskState.PROCESSING_BY_REST_APPLICATION.toString(), topologyName, taskId);
        tasksByStateDAO.insert(state, topologyName, taskId, applicationIdentifier, topicName);
    }


    private void updateTaskStatus(long taskId, HarvestResult harvesterResult) {
        if (harvesterResult.getTaskState() != TaskState.DROPPED && harvesterResult.getResultCounter() == 0) {
            LOGGER.info("Task dropped. No data harvested");
            taskInfoDAO.dropTask(taskId, "The task with the submitted parameters is empty",
                    TaskState.DROPPED.toString());
        } else {
            LOGGER.info("Updating task {} expected size to: {}", taskId, harvesterResult.getResultCounter());
            taskInfoDAO.updateStatusExpectedSize(taskId, harvesterResult.getTaskState().toString(),
                    harvesterResult.getResultCounter());
        }
    }

    /**
     * @return The number of files inside the task.
     */
    private int getFilesCountInsideTask(DpsTask task, String topologyName) throws TaskSubmissionException {
        if (TopologiesNames.HTTP_TOPOLOGY.equals(topologyName)) {
            return UNKNOWN_EXPECTED_SIZE;
        }
        String taskType = getTaskType(task);
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(taskType);
        return filesCounter.getFilesCount(task);
    }

    //get TaskType
    private String getTaskType(DpsTask task) {
        //TODO sholud be done in more error prone way
        final InputDataType first = task.getInputData().keySet().iterator().next();
        return first.name();
    }
}

