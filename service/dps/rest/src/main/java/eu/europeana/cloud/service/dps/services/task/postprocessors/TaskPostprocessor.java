package eu.europeana.cloud.service.dps.services.task.postprocessors;

import eu.europeana.cloud.service.dps.DpsTask;

/**
 *
 */
public interface TaskPostprocessor {

    /**
     * Executes post processing activity for the provided task
     *
     * @param dpsTask
     */
    void execute(DpsTask dpsTask);
}
