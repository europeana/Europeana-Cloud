package eu.europeana.cloud.service.dps.task;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by pwozniak on 10/2/18
 */
public class InitialActionsExecutorFactory {
    @Autowired
    private CassandraTaskInfoDAO taskDAO;

    private static final String INDEXING_TOPOLOGY = "indexing_topology";


    public TaskInitialActionsExecutor get(DpsTask task, String topologyName) {
        if (INDEXING_TOPOLOGY.equals(topologyName)) {
            return new IndexingTaskInitialActionsExecutor(task);
        }
        return new TaskInitialActionsExecutor() {

            @Override
            public void execute() {

            }
        };
    }
}


