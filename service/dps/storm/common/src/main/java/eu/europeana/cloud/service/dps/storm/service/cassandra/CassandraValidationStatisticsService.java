package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.ValidationStatisticsReportService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CassandraValidationStatisticsService implements ValidationStatisticsReportService {

    @Autowired
    private CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;


    /**
     * {@inheritDoc}
     *
     * @param taskId
     */
    @Override
    public StatisticsReport getTaskStatisticsReport(long taskId) {
        StatisticsReport report = cassandraNodeStatisticsDAO.getStatisticsReport(taskId);

        if (report == null) {
            List<NodeStatistics> nodeStatistics = cassandraNodeStatisticsDAO.getNodeStatistics(taskId);
            if (nodeStatistics == null || nodeStatistics.isEmpty()) {
                return null;
            }
            report = new StatisticsReport(taskId, nodeStatistics);
            cassandraNodeStatisticsDAO.storeStatisticsReport(taskId, report);
        }
        return report;
    }

    @Override
    public List<NodeReport> getElementReport(long taskId, String elementPath) {
        return cassandraNodeStatisticsDAO.getElementReport(taskId, elementPath);
    }

}
