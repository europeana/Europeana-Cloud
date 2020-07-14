package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitingTaskWasKilled;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
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

    private TaskStatusChecker taskStatusChecker;

    private DatasetDepublisher depublisher;

    private TaskStatusUpdater statusUpdater;

    public DepublicationService(TaskStatusChecker taskStatusChecker, DatasetDepublisher depublisher, TaskStatusUpdater statusUpdater) {
        this.taskStatusChecker = taskStatusChecker;
        this.depublisher = depublisher;
        this.statusUpdater = statusUpdater;
    }

    public void depublish(SubmitTaskParameters parameters) {
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

    private void waitForFinish(Future<Integer> future, SubmitTaskParameters parameters) throws ExecutionException, InterruptedException, IndexingException, IOException, URISyntaxException {
        waitForAllRecordsRemoved(future, parameters);
        statusUpdater.setTaskCompletelyProcessed(parameters.getTask().getTaskId(), "Dataset was depublished.");
        LOGGER.info("Task {} succeed ", parameters);
    }

    private void waitForAllRecordsRemoved(Future<Integer> future, SubmitTaskParameters parameters) throws InterruptedException, URISyntaxException, IOException, IndexingException, ExecutionException {
        while (true) {
            long recordsLeft = depublisher.getRecordsCount(parameters);
            saveProgress(parameters.getTask().getTaskId(), parameters.getExpectedSize(), recordsLeft);
            checkRemoveInvocationFinished(future, parameters.getExpectedSize());
            if (recordsLeft == 0) {
                return;
            }
            Thread.sleep(PROGRESS_POLLING_PERIOD);
        }

    }

    private void checkRemoveInvocationFinished(Future<Integer> future, long expectedSize) throws InterruptedException, ExecutionException {
        if(future.isDone()){
            int removedCount = future.get();
            if (removedCount != expectedSize) {
                throw new RuntimeException("Removed " + removedCount + "  rows! But expected to remove " + expectedSize);
            }
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
