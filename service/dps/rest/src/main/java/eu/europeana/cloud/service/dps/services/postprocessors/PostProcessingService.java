package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static eu.europeana.cloud.common.model.dps.TaskState.IN_POST_PROCESSING;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;

public class PostProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);

    public static final String SCHEDULE_CRON_RULE_OAI = "2,32 * * * * *";
    public static final String SCHEDULE_CRON_RULE_HTTP = "12,42 * * * * *";
    public static final String SCHEDULE_CRON_RULE_INDEXING = "22,52 * * * * *";

    public static final String MESSAGE_SUCCESSFULLY_POST_PROCESSED = "Successfully post processed task with id={}";
    public static final String MESSAGE_FAILED_POST_PROCESSED = "Could not post process task with id={}";

    private CassandraTaskInfoDAO taskInfoDAO;

    private TasksByStateDAO tasksByStateDAO;

    private HarvestingPostProcessor harvestingPostProcessor;

    private IndexingPostProcessor indexingPostprocessor;

    public PostProcessingService(CassandraTaskInfoDAO taskInfoDAO, TasksByStateDAO tasksByStateDAO,
                                 HarvestingPostProcessor harvestingPostProcessor, IndexingPostProcessor indexingPostprocessor) {
        this.taskInfoDAO = taskInfoDAO;
        this.tasksByStateDAO = tasksByStateDAO;
        this.harvestingPostProcessor = harvestingPostProcessor;
        this.indexingPostprocessor = indexingPostprocessor;
        LOGGER.info("Created post processing service");
    }

    @Scheduled(cron = SCHEDULE_CRON_RULE_OAI)
    public void executeForOAI() {
        findTask(Arrays.asList(IN_POST_PROCESSING, READY_FOR_POST_PROCESSING),
                TopologiesNames.OAI_TOPOLOGY).ifPresent(this::postProcssOAIHttpTask);
    }

    @Scheduled(cron = SCHEDULE_CRON_RULE_HTTP)
    public void executeForHTTP() {
        findTask(Arrays.asList(IN_POST_PROCESSING, READY_FOR_POST_PROCESSING),
                TopologiesNames.HTTP_TOPOLOGY).ifPresent(this::postProcssOAIHttpTask);
    }

    @Scheduled(cron = SCHEDULE_CRON_RULE_INDEXING)
    public void executeForIndexing() {
        findTask(Arrays.asList(IN_POST_PROCESSING, READY_FOR_POST_PROCESSING),
                TopologiesNames.HTTP_TOPOLOGY).ifPresent(this::postProcssIndexingTask);
    }

    public void executeOneTask(long taskId, String topologyName) {
        if(TopologiesNames.OAI_TOPOLOGY.equals(topologyName) || TopologiesNames.HTTP_TOPOLOGY.equals(topologyName)) {
            postProcssOAIHttpTask(taskId);
        } else if(TopologiesNames.INDEXING_TOPOLOGY.equals(topologyName)) {
            postProcssIndexingTask(taskId);
        } else {
            LOGGER.warn("No postprocessing service for {} topology", topologyName);
        }
    }

    private void postProcssOAIHttpTask(long taskId) {
        try {
            harvestingPostProcessor.execute(loadTask(taskId));
            LOGGER.info(MESSAGE_SUCCESSFULLY_POST_PROCESSED, taskId);
        } catch (IOException | TaskInfoDoesNotExistException e) {
            LOGGER.error(MESSAGE_FAILED_POST_PROCESSED, taskId, e);
        }
    }

    private void postProcssIndexingTask(long taskId) {
        try {
            indexingPostprocessor.execute(loadTask(taskId));
            LOGGER.info(MESSAGE_SUCCESSFULLY_POST_PROCESSED, taskId);
        } catch (IOException | TaskInfoDoesNotExistException e) {
            LOGGER.error(MESSAGE_FAILED_POST_PROCESSED, taskId, e);
        }
    }

    private Optional<Long> findTask(List<TaskState> state, String topologyName) {
        LOGGER.info("Finding tasks in {} state...", state);
        Optional<Long> result = tasksByStateDAO.findTaskByStateAndTopology(state, topologyName).map(TaskInfo::getId);

        if (result.isPresent()) {
            LOGGER.info("Found task to post process with id= {}", result.get());
        } else {
            LOGGER.info("There are no tasks in {} state for topology {} on this machine.", state, topologyName);
        }

        return result;
    }

    private DpsTask loadTask(long taskId) throws IOException, TaskInfoDoesNotExistException {
        var taskInfo = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
        var dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
        dpsTask.addParameter(PluginParameterKeys.HARVEST_DATE, DateHelper.getISODateString(taskInfo.getSentDate()));
        return dpsTask;
    }

}
