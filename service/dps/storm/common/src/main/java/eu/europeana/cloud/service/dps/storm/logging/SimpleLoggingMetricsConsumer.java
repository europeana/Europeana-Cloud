
package eu.europeana.cloud.service.dps.storm.logging;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;



public class SimpleLoggingMetricsConsumer implements IMetricsConsumer {
	
    public static final Logger LOG = LoggerFactory.getLogger(SimpleLoggingMetricsConsumer.class);
    
    private final String METRIC_KEY = "word_count";

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) { }

    static private String padding = "                       ";

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
    	
        StringBuilder sb = new StringBuilder();
        String header = String.format("SIMPLE_METRICS %d\t%15s:%-4d\t%3d:%-11s\t",
            taskInfo.timestamp,
            taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
            taskInfo.srcTaskId,
            taskInfo.srcComponentId);
        sb.append(header);
        
        for (DataPoint p : dataPoints) {
        	
        	if (p.value.toString().contains(METRIC_KEY) 
        			|| p.name.toString().contains(METRIC_KEY)
        				|| header.contains(METRIC_KEY)) {
        		
                sb.delete(header.length(), sb.length());
                sb.append(p.name)
                    .append(padding).delete(header.length()+23,sb.length()).append("\t\n")
                    .append(p.value);
                LOG.info(sb.toString());
        	}
        }
    }

    @Override
    public void cleanup() { }
}
