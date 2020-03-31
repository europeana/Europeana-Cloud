package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.service.dps.DpsTask;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by Tarek on 4/26/2019.
 */
public class CollectorWrapper extends SpoutOutputCollector {
    private TaskQueueFiller taskQueueFiller;
    private static final Logger LOGGER = LoggerFactory.getLogger(CollectorWrapper.class);

    public CollectorWrapper(ISpoutOutputCollector delegate, TaskQueueFiller taskQueueFiller) {
        super(delegate);
        this.taskQueueFiller = taskQueueFiller;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        try {
            DpsTask dpsTask = new ObjectMapper().readValue((String) tuple.get(4), DpsTask.class);
            if (dpsTask != null) {
                taskQueueFiller.addNewTask(dpsTask);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        return Collections.emptyList();
    }
}

