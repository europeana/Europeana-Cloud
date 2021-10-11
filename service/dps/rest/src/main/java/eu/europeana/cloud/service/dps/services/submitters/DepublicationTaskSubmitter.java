package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.depublish.DepublicationService;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.DEPUBLICATION_TOPOLOGY;

@Service
public class DepublicationTaskSubmitter implements TaskSubmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationTaskSubmitter.class);

    private final FilesCounterFactory filesCounterFactory;

    private final DepublicationService depublicationService;

    private final TaskStatusUpdater taskStatusUpdater;

    public DepublicationTaskSubmitter(FilesCounterFactory filesCounterFactory, DepublicationService depublicationService, TaskStatusUpdater taskStatusUpdater) {
        this.filesCounterFactory = filesCounterFactory;
        this.depublicationService = depublicationService;
        this.taskStatusUpdater = taskStatusUpdater;
    }

    @Override
    public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {
        evaluateTaskSize(parameters);
        if (parameters.getTaskParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH) != null) {
            depublicationService.depublishIndividualRecords(parameters);
        } else {
            depublicationService.depublishDataset(parameters);
        }
    }

    private void evaluateTaskSize(SubmitTaskParameters parameters) throws TaskSubmissionException {
        LOGGER.info("Evaluating size of depublication task for task_id {} ", parameters.getTask().getTaskId());
        DpsTask task = parameters.getTask();
        int expectedSize = filesCounterFactory.createFilesCounter(task, DEPUBLICATION_TOPOLOGY).getFilesCount(task);
        parameters.getTaskInfo().setExpectedRecordsNumber(expectedSize);
        LOGGER.info("Evaluated size: {} for task id {}", expectedSize, task.getTaskId());
        taskStatusUpdater.updateStatusExpectedSize(task.getTaskId(), TaskState.DEPUBLISHING, expectedSize);
    }


}
