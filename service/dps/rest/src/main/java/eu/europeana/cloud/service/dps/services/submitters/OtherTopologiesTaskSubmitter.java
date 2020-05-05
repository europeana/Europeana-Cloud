package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReader;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSTaskSubmiter;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.dps.structs.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class OtherTopologiesTaskSubmitter implements TaskSubmitter{

    private static final Logger LOGGER = LoggerFactory.getLogger(OtherTopologiesTaskSubmitter.class);

    @Autowired
    private KafkaTopicSelector kafkaTopicSelector;

    @Autowired
    private FilesCounterFactory filesCounterFactory;

    @Autowired
    private TaskStatusChecker taskStatusChecker;

    @Autowired
    private TaskStatusUpdater taskStatusUpdater;

    @Autowired
    private RecordExecutionSubmitService recordSubmitService;

    @Value("${/dps/mcsLocation}")
    private String mcsClientURL;

    @Override
    public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {
        int expectedCount = getFilesCountInsideTask(parameters.getTask(), parameters.getTopologyName());
        LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

        if (expectedCount == 0) {
            taskStatusUpdater.insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),
                    expectedCount, TaskState.DROPPED.toString(), "The task doesn't include any records", "");
            return;
        }

        String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTopologyName());

        taskStatusUpdater.insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),
                expectedCount, TaskState.PROCESSING_BY_REST_APPLICATION.toString(), "Task submitted successfully and processed by REST app", preferredTopicName);

        createMCSReader(parameters.getTopologyName(), parameters.getTask(), preferredTopicName).execute();
        taskStatusUpdater.insertTask(parameters.getTask().getTaskId(), parameters.getTopologyName(),
                expectedCount, TaskState.SENT.toString(), "", "");
    }

    private MCSTaskSubmiter createMCSReader(String topologyName, DpsTask task, String topicName){
        String authorizationHeader = task.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        MCSReader reader=new MCSReader(mcsClientURL,authorizationHeader);
        return new MCSTaskSubmiter(taskStatusChecker,taskStatusUpdater,recordSubmitService,topologyName,task,topicName,reader);
    }

    private int getFilesCountInsideTask(DpsTask task, String topologyName) throws TaskSubmissionException {
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(task, topologyName);
        return filesCounter.getFilesCount(task);
    }
}
