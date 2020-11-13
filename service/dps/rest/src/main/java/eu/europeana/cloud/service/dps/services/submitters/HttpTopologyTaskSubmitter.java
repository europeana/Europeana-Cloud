package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Date;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS;

@Service
public class HttpTopologyTaskSubmitter implements TaskSubmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTopologyTaskSubmitter.class);

    private final TaskExecutionSubmitService submitService;
    private final TaskStatusUpdater taskStatusUpdater;
    private final FilesCounterFactory filesCounterFactory;

    public HttpTopologyTaskSubmitter(TaskExecutionSubmitService submitService,
                                     TaskStatusUpdater taskStatusUpdater,
                                     FilesCounterFactory filesCounterFactory) {
        this.submitService = submitService;
        this.taskStatusUpdater = taskStatusUpdater;
        this.filesCounterFactory = filesCounterFactory;
    }

    @Override
    public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {

        if (parameters.isRestarted()) {
            LOGGER.info("The task {} in Http Topology cannot be restarted.", parameters.getTask().getTaskId());
            taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task in Http Topology cannot be restarted. It will be dropped.");
            return;
        }

        int expectedCount = getFilesCountInsideTask(parameters.getTask(), parameters.getTopologyName());
        LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

        if (expectedCount == 0) {
            taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task doesn't include any records");
            return;
        }

        parameters.getTask().addParameter(MESSAGE_PROCESSING_START_TIME_IN_MS, new Date().getTime() + "");
        submitService.submitTask(parameters.getTask(), parameters.getTopologyName());
        taskStatusUpdater.updateStatusExpectedSize(parameters.getTask().getTaskId(),TaskState.SENT.toString(),expectedCount);
    }

    private int getFilesCountInsideTask(DpsTask task, String topologyName) throws TaskSubmissionException {
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(task, topologyName);
        return filesCounter.getFilesCount(task);
    }
}
