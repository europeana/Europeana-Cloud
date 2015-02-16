
package eu.europeana.cloud.service.dps.storm;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

public class SimpleLoggingMetricsConsumer implements IMetricsConsumer {
	
    public static final Logger LOG = LoggerFactory.getLogger(SimpleLoggingMetricsConsumer.class);

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
            sb.delete(header.length(), sb.length());
            sb.append(p.name)
                .append(padding).delete(header.length()+23,sb.length()).append("\t\n")
                .append(p.value);
            LOG.info(sb.toString());
        }
    }

    @Override
    public void cleanup() { }
}
