package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.TaskExecutionKillService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;

/**
 * Created by Tarek on 2/20/2018.
 */
public class CassandraKillService implements TaskExecutionKillService {
    private CassandraTaskInfoDAO taskDAO;

    public CassandraKillService(CassandraConnectionProvider cassandraConnectionProvider) {
        taskDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
    }

    @Override
    public void killTask(long taskId,String info) {
        taskDAO.setTaskDropped(taskId,info);
    }

}


