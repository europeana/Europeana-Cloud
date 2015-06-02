
package eu.europeana.cloud.service.dps.storm.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetric;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperReportService;

/**
 * Multi count {@link IMetric} that stores Metrics in Zookeeper.
 * 
 * @author manos
 */
public class ZookeeperMultiCountMetric implements IMetric {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperMultiCountMetric.class);

    /**
     * Can be used to update Progress or other information about a currently running Task.
     */
    private TaskExecutionReportService reportService;
    
    public ZookeeperMultiCountMetric(final String zkAddress) {
    	
		reportService = new ZookeeperReportService(zkAddress);
		LOGGER.info("ZookeeperMultiCountMetric started.");
    }
    
    public void incr(long taskId) {
    	reportService.incrTaskProgress("" + taskId);
    }
    
    public Object getValueAndReset() {
    	
    	// TODO: for loop that returns all Metrics for all currently running tasks
        return "";
    }
}
