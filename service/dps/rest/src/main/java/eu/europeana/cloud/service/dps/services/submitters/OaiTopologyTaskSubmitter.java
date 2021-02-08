package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.HarvestResult;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.converters.DpsTaskToHarvestConverter;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OaiTopologyTaskSubmitter implements TaskSubmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OaiTopologyTaskSubmitter.class);

    private final HarvestsExecutor harvestsExecutor;
    private final KafkaTopicSelector kafkaTopicSelector;
    private final TaskStatusUpdater taskStatusUpdater;

    public OaiTopologyTaskSubmitter(HarvestsExecutor harvestsExecutor,
                                    KafkaTopicSelector kafkaTopicSelector,
                                    TaskStatusUpdater taskStatusUpdater
    ) {
        this.harvestsExecutor = harvestsExecutor;
        this.kafkaTopicSelector = kafkaTopicSelector;
        this.taskStatusUpdater = taskStatusUpdater;
    }

    @Override
    public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {

        final List<OaiHarvest> harvestsToByExecuted = new DpsTaskToHarvestConverter()
                .from(parameters.getTask()).stream()
                .map(harvest -> new OaiHarvest(harvest.getUrl(), harvest.getMetadataPrefix(),
                        harvest.getOaiSetSpec(),
                        Optional.ofNullable(harvest.getFrom()).map(Date::toInstant).orElse(null),
                        Optional.ofNullable(harvest.getUntil()).map(Date::toInstant).orElse(null)
                )).collect(Collectors.toList());

        int expectedCount = getFilesCountInsideTask(harvestsToByExecuted, parameters);
        LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

        if (expectedCount == 0) {
            taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task doesn't include any records");
            return;
        }

        String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTopologyName());
        parameters.setTopicName(preferredTopicName);
        parameters.setInfo("Task submitted successfully and processed by REST app");
        parameters.setExpectedSize(expectedCount);
        parameters.setStatus(TaskState.PROCESSING_BY_REST_APPLICATION);
        LOGGER.info("Selected topic name: {} for {}", preferredTopicName, parameters.getTask().getTaskId());
        taskStatusUpdater.insertTask(parameters);

        try {
            HarvestResult harvesterResult;
            harvesterResult = harvestsExecutor.execute(harvestsToByExecuted, parameters);
            updateTaskStatus(parameters.getTask().getTaskId(), harvesterResult);
        } catch (HarvesterException e) {
            throw new TaskSubmissionException("Unable to submit task properly", e);
        }
    }

    /**
     * @return The number of files inside the task.
     */
    private int getFilesCountInsideTask(List<OaiHarvest> harvestsToByExecuted, SubmitTaskParameters parameters) throws TaskSubmissionException {
        final OAIPMHHarvestingDetails harvest = parameters.getTask().getHarvestingDetails();
        if (harvest.getExcludedSets() != null && !harvest.getExcludedSets().isEmpty() ||
                harvest.getExcludedSchemas() != null && !harvest.getExcludedSchemas().isEmpty()) {
            LOGGER.info("Cannot count completeListSize for taskId= {} . Excluded sets or schemas are not supported", parameters.getTask().getTaskId());
            return -1;
        }
        int total = 0;
        final OaiHarvester harvester = HarvesterFactory.createOaiHarvester();
        for (OaiHarvest oaiHarvest : harvestsToByExecuted) {
            try {
                final Integer count = harvester.countRecords(oaiHarvest);
                if (count == null) {
                    LOGGER.info(
                            "Cannot count completeListSize for taskId= {}: No resumption token information found.",
                            parameters.getTask().getTaskId());
                    return -1;
                }
                total += count;
            } catch (HarvesterException e) {
                String logMessage = "Cannot complete the request for the following repository URL "
                        + oaiHarvest.getRepositoryUrl();
                LOGGER.info(logMessage, e);
                throw new TaskSubmissionException(logMessage + " Because: " + e.getMessage(), e);
            }
        }
        return total;
    }

    private void updateTaskStatus(long taskId, HarvestResult harvesterResult) {
        if (harvesterResult.getTaskState() != TaskState.DROPPED && harvesterResult.getResultCounter() == 0) {
            LOGGER.info("Task dropped. No data harvested");
            taskStatusUpdater.setTaskDropped(taskId, "The task with the submitted parameters is empty");
        } else {
            LOGGER.info("Updating task {} expected size to: {}", taskId, harvesterResult.getResultCounter());
            taskStatusUpdater.updateStatusExpectedSize(taskId, harvesterResult.getTaskState().toString(),
                    harvesterResult.getResultCounter());
        }
    }
}
