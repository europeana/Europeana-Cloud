package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.ValidationStaticsReportService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class CassandraValidationStatisticsService implements ValidationStaticsReportService {

    @Autowired
    private CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;


    /**
     * {@inheritDoc}
     * @param taskId
     */
    @Override
    public StatisticsReport getTaskStatisticsReport(long taskId) {
        List<NodeStatistics> nodeStatistics = cassandraNodeStatisticsDAO.getNodeStatistics(taskId);
        for(NodeStatistics ns : nodeStatistics){
            System.out.println(ns);
        }
        return new StatisticsReport(taskId, nodeStatistics);
    }

}
