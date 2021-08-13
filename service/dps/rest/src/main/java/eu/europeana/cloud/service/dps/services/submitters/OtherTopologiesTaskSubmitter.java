package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OtherTopologiesTaskSubmitter implements TaskSubmitter{

    private static final Logger LOGGER = LoggerFactory.getLogger(OtherTopologiesTaskSubmitter.class);

    @Autowired
    private KafkaTopicSelector kafkaTopicSelector;

    @Autowired
    private FilesCounterFactory filesCounterFactory;

    @Autowired
    private TaskStatusUpdater taskStatusUpdater;

    @Autowired
    private MCSTaskSubmitter mcsTaskSubmitter;

    @Override
    public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {
        int expectedCount = getFilesCountInsideTask(parameters.getTask(), parameters.getTopologyName());
        LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

        if (expectedCount == 0) {
            taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(),"The task doesn't include any records");
            return;
        }

        String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTopologyName());
        parameters.setTopicName(preferredTopicName);
        parameters.setExpectedRecordsNumber(expectedCount);
        taskStatusUpdater.updateSubmitParameters(parameters);

        mcsTaskSubmitter.execute(parameters);
    }

    private int getFilesCountInsideTask(DpsTask task, String topologyName) throws TaskSubmissionException {
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(task, topologyName);
        return filesCounter.getFilesCount(task);
    }
}
