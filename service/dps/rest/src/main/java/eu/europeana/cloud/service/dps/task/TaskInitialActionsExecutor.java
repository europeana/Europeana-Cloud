package eu.europeana.cloud.service.dps.task;

/**
 * Executes different jobs that has to be executed before dps task will be submitted to the topology.
 */
public interface TaskInitialActionsExecutor {

    void execute() throws InitialActionException;
}
