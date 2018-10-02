package eu.europeana.cloud.service.dps.task;

import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Created by pwozniak on 10/2/18
 */
public class InitialActionsExecutorFactory {

    public static TaskInitialActionsExecutor get(DpsTask task, String topologyName) {
        if (topologyName.equals("oai_topology"))
            return new IndexingTaskInitialActionsExecutor(task, topologyName);

        return new TaskInitialActionsExecutor() {

            @Override
            public void execute() {

            }
        };
    }
}
