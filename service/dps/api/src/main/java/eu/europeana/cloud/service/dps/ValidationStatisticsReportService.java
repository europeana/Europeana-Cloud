package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.dps.StatisticsReport;


/**
 * Service to get statistics report for Task Executions when task was performing validation step.
 */
public interface ValidationStatisticsReportService {

    /**
     * Returns statistics report for a given task. The report is only available if validation was run and there were correctly validated records.
     *
     * @param taskId task identifier
     * @return statistics report
     */
    StatisticsReport getTaskStatisticsReport(long taskId);


}


