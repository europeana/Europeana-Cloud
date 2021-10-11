package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;

import java.util.Set;

/**
 *
 */
public interface TaskPostProcessor {

    /**
     * Executes post processing activity for the provided task
     *
     */
    void execute(TaskInfo taskInfo, DpsTask dpsTask);

    /**
     * Returns set of names of processed topologies
     */
    Set<String> getProcessedTopologies();
}
