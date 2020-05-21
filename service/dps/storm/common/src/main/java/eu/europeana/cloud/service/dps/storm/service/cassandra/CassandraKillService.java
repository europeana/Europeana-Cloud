package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.TaskExecutionKillService;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;

/**
 * Created by Tarek on 2/20/2018.
 */
public class CassandraKillService implements TaskExecutionKillService {
    private TaskStatusUpdater taskStatusUpdater;

    public CassandraKillService(CassandraConnectionProvider cassandraConnectionProvider) {
        taskStatusUpdater = TaskStatusUpdater.getInstance(cassandraConnectionProvider);
    }

    @Override
    public void killTask(long taskId,String info) {
        taskStatusUpdater.setTaskDropped(taskId,info);
    }

}


