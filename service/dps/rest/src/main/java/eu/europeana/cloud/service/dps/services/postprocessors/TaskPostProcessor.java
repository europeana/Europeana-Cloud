package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.service.dps.DpsTask;

import java.util.Set;

/**
 *
 */
public interface TaskPostProcessor {

    /**
     * Executes post processing activity for the provided task
     *
     * @param dpsTask
     */
    void execute(DpsTask dpsTask);

    /**
     * Retrurns set of names of processed topologies
     * @return
     */
    Set<String> getProcessedTopologies();
}
