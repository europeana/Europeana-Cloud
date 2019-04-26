package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Created by Tarek on 4/26/2019.
 */
public interface TaskQueueFiller {
    void addNewTask(DpsTask dpsTask);
}
