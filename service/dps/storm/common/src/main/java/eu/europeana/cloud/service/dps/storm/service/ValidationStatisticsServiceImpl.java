package eu.europeana.cloud.service.dps.storm.service;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.GeneralStatistics;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.service.dps.ValidationStatisticsService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraAttributeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.GeneralStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.dao.StatisticsReportDAO;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class ValidationStatisticsServiceImpl implements ValidationStatisticsService {

  public static final int ELEMENTS_SAMPLE_MAX_SIZE = 100;
  public static final int ATTRIBUTES_SAMPLE_MAX_SIZE = 25;
  public static final int ATTRIBUTES_MAX_ALLOWED_VALUES = 15;
  private static ValidationStatisticsServiceImpl instance;

  private GeneralStatisticsDAO generalStatisticsDAO;
  private CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;
  private CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO;
  private StatisticsReportDAO statisticsReportDAO;

  public ValidationStatisticsServiceImpl() {
  }

  public ValidationStatisticsServiceImpl(GeneralStatisticsDAO generalStatisticsDAO,
      CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO,
      CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO, StatisticsReportDAO statisticsReportDAO) {
    this.generalStatisticsDAO = generalStatisticsDAO;
    this.cassandraNodeStatisticsDAO = cassandraNodeStatisticsDAO;
    this.cassandraAttributeStatisticsDAO = cassandraAttributeStatisticsDAO;
    this.statisticsReportDAO = statisticsReportDAO;
  }

  public static synchronized ValidationStatisticsServiceImpl getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = new ValidationStatisticsServiceImpl(
          GeneralStatisticsDAO.getInstance(cassandra),
          CassandraNodeStatisticsDAO.getInstance(cassandra),
          CassandraAttributeStatisticsDAO.getInstance(cassandra),
          StatisticsReportDAO.getInstance(cassandra)
      );
    }
    return instance;
  }


  /**
   * {@inheritDoc}
   *
   * @param taskId
   */
  @Override
  public StatisticsReport getTaskStatisticsReport(long taskId) {
    StatisticsReport report = statisticsReportDAO.getStatisticsReport(taskId);

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
    for (NodeStatistics nodeStatistics : cassandraNodeStatisticsDAO.getNodeStatistics(taskId, null, nodeXpath,
        ELEMENTS_SAMPLE_MAX_SIZE)) {
      String elementValue = nodeStatistics.getValue();
      List<AttributeStatistics> attributeStatistics =
          new ArrayList<>(cassandraAttributeStatisticsDAO.getAttributeStatistics(
              taskId, nodeXpath, elementValue, ATTRIBUTES_SAMPLE_MAX_SIZE
          ));
      NodeReport nodeValues = new NodeReport(elementValue, nodeStatistics.getOccurrence(), attributeStatistics);
      result.add(nodeValues);
    }
    return result;
  }

  /**
   * Insert statistics for all nodes on the list
   *
   * @param taskId task identifier
   * @param nodes list of node statistics objects
   */
  public void insertNodeStatistics(long taskId, List<NodeStatistics> nodes) {
    for (NodeStatistics nodeStatistics : nodes) {
      insertNodeStatistics(taskId, nodeStatistics);
    }
  }

  /**
   * Insert statistics for the specified node
   *
   * @param taskId task identifier
   * @param nodeStatistics node statistics to insert
   */
  private void insertNodeStatistics(long taskId, NodeStatistics nodeStatistics) {
    generalStatisticsDAO.updateGeneralStatistics(taskId, nodeStatistics);
    // store node statistics only for nodes with values, occurrence of the node itself will be taken from the general statistics
    if (nodeStatistics.getValue() != null) {
      cassandraNodeStatisticsDAO.updateNodeStatistics(taskId, nodeStatistics);
    }
    if (nodeStatistics.hasAttributes()) {
      insertAttributeStatistics(taskId, nodeStatistics.getXpath(), nodeStatistics.getValue(),
          nodeStatistics.getAttributesStatistics());
    }
  }

  /**
   * Inserts the statistics for all the attributes in the given list.
   *
   * @param taskId task identifier
   * @param attributes list of attribute statistics
   */
  public void insertAttributeStatistics(long taskId, String nodeXpath, String nodeValue, Set<AttributeStatistics> attributes) {
    for (AttributeStatistics attributeStatistics : attributes) {
      long distinctValuesCount = cassandraAttributeStatisticsDAO.getAttributeDistinctValues(taskId, nodeXpath, nodeValue,
          attributeStatistics.getName());
      if (distinctValuesCount >= ATTRIBUTES_MAX_ALLOWED_VALUES) {
        long currentCount = cassandraAttributeStatisticsDAO.getSpecificAttributeValueCount(taskId, nodeXpath, nodeValue,
            attributeStatistics.getName(), attributeStatistics.getValue());
        if (currentCount > 0) {
          cassandraAttributeStatisticsDAO.insertAttributeStatistics(taskId, nodeXpath, nodeValue, attributeStatistics);
        }
      } else {
        cassandraAttributeStatisticsDAO.insertAttributeStatistics(taskId, nodeXpath, nodeValue, attributeStatistics);
      }
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

    for (GeneralStatistics generalStatistics : generalStatisticsDAO.searchGeneralStatistics(taskId)) {
      List<NodeStatistics> nodeStatistics = getNodeStatistics(taskId, generalStatistics.getParentXpath(),
          generalStatistics.getNodeXpath());
      if (nodeStatistics.isEmpty()) {
        // this case happens when there is a node without a value but it may contain attributes
        NodeStatistics node = new NodeStatistics(generalStatistics.getParentXpath(), generalStatistics.getNodeXpath(),
            "", generalStatistics.getOccurrence());
        node.setAttributesStatistics(
            cassandraAttributeStatisticsDAO.getAttributeStatistics(taskId, generalStatistics.getNodeXpath(), "", 2));
      }
      result.addAll(nodeStatistics);
    }
    return result;
  }


  private List<NodeStatistics> getNodeStatistics(long taskId, String parentXpath, String nodeXpath) {
    List<NodeStatistics> nodeStatisticsList = cassandraNodeStatisticsDAO.getNodeStatistics(taskId, parentXpath, nodeXpath, 2);
    nodeStatisticsList.forEach(
        nodeStatistics -> nodeStatistics
            .setAttributesStatistics(
                cassandraAttributeStatisticsDAO
                    .getAttributeStatistics(taskId, nodeXpath, nodeStatistics.getValue(), 2)
            )
    );
    return nodeStatisticsList;
  }

  /**
   * Store the StatisticsReport object in the database.
   *
   * @param taskId task identifier
   * @param report report object to store
   */
  void storeStatisticsReport(long taskId, StatisticsReport report) {
    if (statisticsReportDAO.isReportStored(taskId)) {
      return;
    }
    statisticsReportDAO.storeReport(taskId, report);
  }

  public void removeStatistics(long taskId) {
    removeGeneralStatistics(taskId);
    statisticsReportDAO.removeStatisticsReport(taskId);
  }


  private void removeGeneralStatistics(long taskId) {

    for (GeneralStatistics s : generalStatisticsDAO.searchGeneralStatistics(taskId)) {
      removeNodeStatistics(taskId, s.getNodeXpath());
    }
    generalStatisticsDAO.removeGeneralStatistics(taskId);
  }

  private void removeNodeStatistics(long taskId, String nodeXpath) {

    for (String value : cassandraNodeStatisticsDAO.searchNodeStatisticsValues(taskId, nodeXpath)) {
      cassandraAttributeStatisticsDAO.removeAttributeStatistics(taskId, nodeXpath, value);
    }
    cassandraNodeStatisticsDAO.removeNodeStatistics(taskId, nodeXpath);
  }

}
