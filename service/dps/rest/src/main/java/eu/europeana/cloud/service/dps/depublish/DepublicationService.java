package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitingTaskWasKilled;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.indexing.exception.IndexingException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class DepublicationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationService.class);
    private static final long PROGRESS_POLLING_PERIOD = 5_000;

    @Autowired
    private TaskStatusChecker taskStatusChecker;

    @Autowired
    private DatasetDepublisher depublisher;

    @Autowired
    private TaskStatusUpdater statusUpdater;

    public void depublish(SubmitTaskParameters parameters) {
        try {
            long taskId = parameters.getTask().getTaskId();
            boolean useAlternativeEnvironment = Boolean.parseBoolean(parameters.getTask().getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV));
            String datasetMetisId = parameters.getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID);

            long expectedSize = evaluateExpectedSize(taskId, datasetMetisId, useAlternativeEnvironment);
            checkTaskKilled(taskId);
            Future<Integer> future = depublisher.executeDatasetDepublicationAsync(datasetMetisId, useAlternativeEnvironment);
            waitForFinish(taskId, future, datasetMetisId, useAlternativeEnvironment, expectedSize);

        } catch (SubmitingTaskWasKilled e) {
            LOGGER.warn(e.getMessage(), e);
        } catch (Exception e) {
            saveErrorResult(parameters, e);
        }
    }

    private long evaluateExpectedSize(long taskId, String datasetMetisId, boolean useAlternativeEnvironment) throws URISyntaxException, IOException, IndexingException {
        statusUpdater.updateTask(taskId, "Evaluating dataset size", TaskState.PENDING.toString(), new Date());
        LOGGER.info("Started depublication task id {} datasetMetisId {} useAlternativeEnvironment {}",
                taskId, datasetMetisId, useAlternativeEnvironment);

        long expectedSize = depublisher.getRecordsCount(datasetMetisId, useAlternativeEnvironment);

        if (expectedSize > Integer.MAX_VALUE) {
            throw new RuntimeException("There are " + expectedSize + " records in set. It exceeds Integer size and is not supported.");
        }
        if (expectedSize <= 0) {
            throw new RuntimeException("Not found any records of dataset with metis id = " + datasetMetisId);
        }

        statusUpdater.updateStatusExpectedSize(taskId, TaskState.DEPUBLISHING.toString(), (int) expectedSize);
        LOGGER.info("Evaluated size {} for task id {}", expectedSize, taskId);
        return expectedSize;
    }

    private void waitForFinish(long taskId, Future<Integer> future, String datasetMetisId, boolean useAlternativeEnvironment, long expectedSize) throws ExecutionException, InterruptedException, IndexingException, IOException, URISyntaxException {
        waitForRemovingInvocationFinish(taskId, future, datasetMetisId, useAlternativeEnvironment, expectedSize);
        waitForAllRecordsRemoved(taskId, datasetMetisId, useAlternativeEnvironment, expectedSize);
        statusUpdater.setTaskCompletelyProcessed(taskId, "Dataset was depublished.");
        LOGGER.info("Task {} succeed ", taskId);
    }

    private void waitForRemovingInvocationFinish(long taskId, Future<Integer> future, String datasetMetisId, boolean useAlternativeEnvironment, long expectedSize) throws InterruptedException, ExecutionException, URISyntaxException, IOException, IndexingException {
        while (true) {
            try {
                int removedCount = future.get(PROGRESS_POLLING_PERIOD, TimeUnit.MILLISECONDS);
                if (removedCount != expectedSize) {
                    throw new RuntimeException("Removed " + removedCount + "  rows! But expected to remove " + expectedSize);
                }
                return;
            } catch (TimeoutException e) {
                long recordsLeft = depublisher.getRecordsCount(datasetMetisId, useAlternativeEnvironment);
                saveProgress(taskId, expectedSize, recordsLeft);
            }
        }
    }

    private void waitForAllRecordsRemoved(long taskId, String datasetMetisId, boolean useAlternativeEnvironment, long expectedSize) throws InterruptedException, URISyntaxException, IOException, IndexingException {
        while (true) {
            long recordsLeft = depublisher.getRecordsCount(datasetMetisId, useAlternativeEnvironment);
            saveProgress(taskId, expectedSize, recordsLeft);
            if (recordsLeft == 0) {
                return;
            }
            Thread.sleep(PROGRESS_POLLING_PERIOD);
        }

    }

    private void checkTaskKilled(long taskId) {
        if (taskStatusChecker.hasKillFlag(taskId)) {
            throw new SubmitingTaskWasKilled(taskId);
        }
    }

    private void saveProgress(long taskId, long expectedSize, long recordsLeft) {
        long processed = expectedSize - recordsLeft;
        statusUpdater.setUpdateProcessedFiles(taskId, (int) processed, 0);
    }

    private void saveErrorResult(SubmitTaskParameters parameters, Exception e) {
        String fullStacktrace = ExceptionUtils.getStackTrace(e);
        LOGGER.error("Task execution failed: {}", fullStacktrace);
        statusUpdater.setTaskDropped(parameters.getTask().getTaskId(), fullStacktrace);
    }

}
