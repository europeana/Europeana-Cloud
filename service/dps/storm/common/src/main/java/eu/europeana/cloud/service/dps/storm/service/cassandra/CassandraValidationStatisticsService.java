package eu.europeana.cloud.service.dps.storm.service.cassandra;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.GeneralStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.ValidationStatisticsReportService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraAttributeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraGeneralStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraStatisticsReportDAO;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class CassandraValidationStatisticsService implements ValidationStatisticsReportService {

    public static final int ELEMENTS_SAMPLE_MAX_SIZE = 100;
    public static final int ATTRIBUTES_SAMPLE_MAX_SIZE = 25;
    private static CassandraValidationStatisticsService instance;

    private CassandraGeneralStatisticsDAO cassandraGeneralStatisticsDAO;
    private CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;
    private CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO;
    private CassandraStatisticsReportDAO cassandraStatisticsReportDAO;

    public static synchronized CassandraValidationStatisticsService getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new CassandraValidationStatisticsService(
                    CassandraGeneralStatisticsDAO.getInstance(cassandra),
                    CassandraNodeStatisticsDAO.getInstance(cassandra),
                    CassandraAttributeStatisticsDAO.getInstance(cassandra),
                    CassandraStatisticsReportDAO.getInstance(cassandra)
            );
        }
        return instance;
    }

    public CassandraValidationStatisticsService() {
    }

    public CassandraValidationStatisticsService(CassandraGeneralStatisticsDAO cassandraGeneralStatisticsDAO, CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO, CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO, CassandraStatisticsReportDAO cassandraStatisticsReportDAO) {
        this.cassandraGeneralStatisticsDAO = cassandraGeneralStatisticsDAO;
        this.cassandraNodeStatisticsDAO = cassandraNodeStatisticsDAO;
        this.cassandraAttributeStatisticsDAO = cassandraAttributeStatisticsDAO;
        this.cassandraStatisticsReportDAO = cassandraStatisticsReportDAO;
    }

    /**
     * {@inheritDoc}
     *
     * @param taskId
     */
    @Override
    public StatisticsReport getTaskStatisticsReport(long taskId) {
        StatisticsReport report = cassandraStatisticsReportDAO.getStatisticsReport(taskId);

        if (report == null) {
            List<NodeStatistics> nodeStatistics = getNodeStatistics(taskId);
            if (nodeStatistics == null || nodeStatistics.isEmpty()) {
                return null;
            }
            report = new StatisticsReport(taskId, nodeStatistics);
            storeStatisticsReport(taskId, report);
        }
        return report;
    }

    @Override
    public List<NodeReport> getElementReport(long taskId, String nodeXpath) {
        List<NodeReport> result = new ArrayList<>();
        for(NodeStatistics nodeStatistics:cassandraNodeStatisticsDAO.getNodeStatistics(taskId, null, nodeXpath, ELEMENTS_SAMPLE_MAX_SIZE)){
            String elementValue = nodeStatistics.getValue();
            List<AttributeStatistics> attributeStatistics = new ArrayList<>(cassandraAttributeStatisticsDAO.getAttributeStatistics(taskId, nodeXpath, elementValue, ATTRIBUTES_SAMPLE_MAX_SIZE ));
            NodeReport nodeValues = new NodeReport(elementValue, nodeStatistics.getOccurrence(), attributeStatistics);
            result.add(nodeValues);
        }
        return result;
    }

    /**
     * Insert statistics for all nodes on the list
     *
     * @param taskId task identifier
     * @param nodes  list of node statistics objects
     */
    public void insertNodeStatistics(long taskId, List<NodeStatistics> nodes) {
        for (NodeStatistics nodeStatistics : nodes) {
            insertNodeStatistics(taskId, nodeStatistics);
        }
    }

    /**
     * Insert statistics for the specified node
     *
     * @param taskId         task identifier
     * @param nodeStatistics node statistics to insert
     */
    private void insertNodeStatistics(long taskId, NodeStatistics nodeStatistics) {
        cassandraGeneralStatisticsDAO.updateGeneralStatistics(taskId, nodeStatistics);
        // store node statistics only for nodes with values, occurrence of the node itself will be taken from the general statistics
        if (nodeStatistics.getValue() != null) {
            cassandraNodeStatisticsDAO.updateNodeStatistics(taskId, nodeStatistics);
        }
        if (nodeStatistics.hasAttributes()) {
            cassandraAttributeStatisticsDAO.insertAttributeStatistics(taskId, nodeStatistics.getXpath(), nodeStatistics.getValue(), nodeStatistics.getAttributesStatistics());
        }
    }



    /**
     * Get all statistics for the specified task
     *
     * @param taskId task identifier
     * @return list of all node statistics
     */
    public List<NodeStatistics> getNodeStatistics(long taskId) {
        List<NodeStatistics> result = new ArrayList<>();

        for (GeneralStatistics generalStatistics : cassandraGeneralStatisticsDAO.searchGeneralStatistics(taskId)) {
            List<NodeStatistics> nodeStatistics = getNodeStatistics(taskId, generalStatistics.getParentXpath(), generalStatistics.getNodeXpath());
            if (nodeStatistics.isEmpty()) {
                // this case happens when there is a node without a value but it may contain attributes
                NodeStatistics node = new NodeStatistics(generalStatistics.getParentXpath(), generalStatistics.getNodeXpath(), "", generalStatistics.getOccurrence());
                node.setAttributesStatistics(cassandraAttributeStatisticsDAO.getAttributeStatistics(taskId, generalStatistics.getNodeXpath(), "", 2));
            }
            result.addAll(nodeStatistics);
        }
        return result;
    }


    private List<NodeStatistics> getNodeStatistics(long taskId, String parentXpath, String nodeXpath) {
        List<NodeStatistics> nodeStatisticsList = cassandraNodeStatisticsDAO.getNodeStatistics(taskId, parentXpath, nodeXpath, 2);
        nodeStatisticsList.forEach(nodeStatistics->nodeStatistics.setAttributesStatistics(cassandraAttributeStatisticsDAO.getAttributeStatistics(taskId, nodeXpath, nodeStatistics.getValue(), 2)));
        return nodeStatisticsList;
    }

    /**
     * Store the StatisticsReport object in the database.
     *
     * @param taskId task identifier
     * @param report report object to store
     */
    void storeStatisticsReport(long taskId, StatisticsReport report) {
        if (cassandraStatisticsReportDAO.isReportStored(taskId)) {
            return;
        }
        cassandraStatisticsReportDAO.storeReport(taskId, report);
    }

    public void removeStatistics(long taskId) {
        removeGeneralStatistics(taskId);
        cassandraStatisticsReportDAO.removeStatisticsReport(taskId);
    }


    private void removeGeneralStatistics(long taskId) {

        for (GeneralStatistics s:cassandraGeneralStatisticsDAO.searchGeneralStatistics(taskId)) {
            removeNodeStatistics(taskId, s.getNodeXpath());
        }
        cassandraGeneralStatisticsDAO.removeGeneralStatistics(taskId);
    }

    private void removeNodeStatistics(long taskId, String nodeXpath) {

        for(String value:cassandraNodeStatisticsDAO.searchNodeStatisticsValues(taskId,nodeXpath)){
            cassandraAttributeStatisticsDAO.removeAttributeStatistics(taskId, nodeXpath, value);
        }
        cassandraNodeStatisticsDAO.removeNodeStatistics(taskId, nodeXpath);
    }

}
