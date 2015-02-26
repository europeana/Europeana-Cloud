
package eu.europeana.cloud.service.dps.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetric;
import eu.europeana.cloud.service.coordination.ZookeeperService;
import eu.europeana.cloud.service.coordination.configuration.DynamicPropertyManager;
import eu.europeana.cloud.service.coordination.configuration.ZookeeperDynamicPropertyManager;

/**
 * Multi count {@link IMetric} that stores Metrics in Zookeeper.
 * 
 * Metrics are stored in Zookeeper directories as direct ZK child nodes of the root parent ZK node 
 * (which is specified in {@link #ZOOKEEPER_PATH}).
 * 
 * Example:
 * 
 * An example Metric property node is "/eCloud/v2/ISTI/configuration/tasks/xslt-32427389328/progress".
 * For the above example, an example-value for Metric "progress", for task "xslt-32427389328" would be "50",
 * that indicates that current progress is at 50%.
 * 
 * @author manos
 */
public class ZookeeperMultiCountMetric implements IMetric {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperMultiCountMetric.class);

    private final static int ZOOKEEPER_CONNECTION_TIME = 3000;
    private final static int ZOOKEEPER_SESSION_TIMEOUT = 3000;
    private final static String ZOOKEEPER_ADDRESS = "ecloud.eanadev.org:2181";
    
	/** Base path where all metrics are stored. */
    private final static String ZOOKEEPER_PATH = "/eCloud/v2/ISTI/pro/configuration/tasks";
    
    /**
     * Can be used to set / retrieve Dynamic Properties.
     */
    private DynamicPropertyManager pManager;
    
    public ZookeeperMultiCountMetric() {
    	
		ZookeeperService zS = new ZookeeperService(ZOOKEEPER_ADDRESS,
				ZOOKEEPER_CONNECTION_TIME, ZOOKEEPER_SESSION_TIMEOUT, ZOOKEEPER_PATH);
		
		pManager = new ZookeeperDynamicPropertyManager(zS, ZOOKEEPER_PATH);
		
		LOGGER.info("ZookeeperMultiCountMetric started.");
    }
    
    /**
     * Increases the current value of the specified @param metric
     * by 1.
     * 
     * @param taskId Metric belongs to this taskId
     * @param taskName Metric belongs to task with this taskName
     * 
     * TaskId and TaskName are used to generate the path where the metric would be stored.
     */
    public void incr(long taskId, String taskName, String metric) {

    	final String key = taskName + "-" + taskId  + "/" + metric;
    	String currentValue = pManager.getCurrentValue(key);
    	
    	if (currentValue == null) {
    		currentValue = "0";
    	}
    	
    	int c = Integer.parseInt(currentValue);
    	c += 1;
    	
    	pManager.updateValue(key, c + "");
    }

    public Object getValueAndReset() {
    	
    	// TODO: for loop that returns all Metrics for all currently running tasks
        return "";
    }
}
