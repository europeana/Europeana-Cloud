package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.services.submitters.SubmitingTaskWasKilled;
import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.indexing.exception.IndexingException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class DepublicationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationService.class);
    private static final long PROGRESS_POLLING_PERIOD = 5_000;

    private final TaskStatusChecker taskStatusChecker;
    private final DatasetDepublisher depublisher;
    private final TaskStatusUpdater taskStatusUpdater;
    private final RecordStatusUpdater recordStatusUpdater;


    public DepublicationService(TaskStatusChecker taskStatusChecker, DatasetDepublisher depublisher, TaskStatusUpdater taskStatusUpdater, RecordStatusUpdater recordStatusUpdater) {
        this.taskStatusChecker = taskStatusChecker;
        this.depublisher = depublisher;
        this.taskStatusUpdater = taskStatusUpdater;
        this.recordStatusUpdater = recordStatusUpdater;
    }

    public void depublishDataset(SubmitTaskParameters parameters) {
        try {
            long taskId = parameters.getTask().getTaskId();
            checkTaskKilled(taskId);
            Future<Integer> future = depublisher.executeDatasetDepublicationAsync(parameters);
            waitForFinish(future, parameters);

        } catch (SubmitingTaskWasKilled e) {
            LOGGER.warn(e.getMessage(), e);
        } catch (Exception e) {
            saveErrorResult(parameters, e);
        }
    }

    public void depublishIndividualRecords(SubmitTaskParameters parameters) {
        LOGGER.info("Removing individual records from index");
        String[] records = parameters.getTaskParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH).split(",");
        int errors = 0;
        for (int i = 0; i < records.length; i++) {
            int resourceNum = i + 1;
            try {
                LOGGER.info("Removing record with id '{}' from index", records[i]);
                checkTaskKilled(parameters.getTask().getTaskId());
                boolean removedSuccessfully = depublisher.removeRecord(parameters, records[i]);
                if (removedSuccessfully) {
                    recordStatusUpdater.addSuccessfullyProcessedRecord(resourceNum, parameters.getTask().getTaskId(),
                            TopologiesNames.DEPUBLICATION_TOPOLOGY, records[i]);
                } else {
                    recordStatusUpdater.addWronglyProcessedRecord(resourceNum, parameters.getTask().getTaskId(),
                            TopologiesNames.DEPUBLICATION_TOPOLOGY, records[i], null, null);
                    errors++;
                }

            } catch (SubmitingTaskWasKilled e) {
                LOGGER.warn(e.getMessage(), e);
                return;
            } catch (Exception e) {
                LOGGER.warn("Error while depublishing record {}" , records[i], e);
                recordStatusUpdater.addWronglyProcessedRecord(
                        resourceNum,
                        parameters.getTask().getTaskId(),
                        TopologiesNames.DEPUBLICATION_TOPOLOGY,
                        records[i], e.getMessage(), ExceptionUtils.getStackTrace(e));
                errors++;
            }
            saveProgress(parameters.getTask().getTaskId(), resourceNum, errors);
        }
        taskStatusUpdater.setTaskCompletelyProcessed(parameters.getTask().getTaskId(), "Dataset was depublished.");
        LOGGER.info("Records removal procedure finished for task_id {}", parameters.getTask().getTaskId());
    }

    private void waitForFinish(Future<Integer> future, SubmitTaskParameters parameters) throws ExecutionException, InterruptedException, IndexingException, IOException, URISyntaxException {
        waitForAllRecordsRemoved(future, parameters);
        taskStatusUpdater.setTaskCompletelyProcessed(parameters.getTask().getTaskId(), "Dataset was depublished.");
        LOGGER.info("Task {} succeed ", parameters);
    }

    private void waitForAllRecordsRemoved(Future<Integer> future, SubmitTaskParameters parameters) throws InterruptedException, URISyntaxException, IOException, IndexingException, ExecutionException {
        while (true) {
            long recordsLeft = depublisher.getRecordsCount(parameters);
            saveProgress(parameters.getTask().getTaskId(), parameters.getExpectedSize() - recordsLeft);
            checkRemoveInvocationFinished(future, parameters.getExpectedSize());
            if (recordsLeft == 0) {
                return;
            }
            Thread.sleep(PROGRESS_POLLING_PERIOD);
        }

    }

    private void checkRemoveInvocationFinished(Future<Integer> future, long expectedSize) throws InterruptedException, ExecutionException {
        if (future.isDone()) {
            int removedCount = future.get();
            if (removedCount != expectedSize) {
                throw new DepublicationException("Removed " + removedCount + "  rows! But expected to remove " + expectedSize);
            }
        }
    }

    private void checkTaskKilled(long taskId) {
        if (taskStatusChecker.hasKillFlag(taskId)) {
            throw new SubmitingTaskWasKilled(taskId);
        }
    }

    private void saveProgress(long taskId, long processed) {
        saveProgress(taskId, (int) processed, 0);
    }

    private void saveProgress(long taskId, int processed, int errors) {
        taskStatusUpdater.setUpdateProcessedFiles(taskId, processed, errors);
    }

    private void saveErrorResult(SubmitTaskParameters parameters, Exception e) {
        String fullStacktrace = ExceptionUtils.getStackTrace(e);
        LOGGER.error("Task execution failed: {}", fullStacktrace);
        taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), fullStacktrace);
    }

}
