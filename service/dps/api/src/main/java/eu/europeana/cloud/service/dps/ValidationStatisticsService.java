package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.StatisticsReport;

import java.util.List;


/**
 * Service to get statistics report for Task Executions when task was performing validation step.
 */
public interface ValidationStatisticsService {

  /**
   * Returns statistics report for a given task. The report is only available if validation was run and there were correctly
   * validated records.
   *
   * @param taskId task identifier
   * @return statistics report
   */
  StatisticsReport getTaskStatisticsReport(long taskId);


  /**
   * Retrieves a list of distinct values and their occurrences for a specific element based on its path}.
   *
   * @param taskId task identifier
   * @param elementPath element path
   * @return List of distinct values and their occurrences for a specific element
   */
  List<NodeReport> getElementReport(long taskId, String elementPath);


}


