package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.throttling.ThrottlingAttributeGenerator;
import eu.europeana.cloud.service.dps.storm.utils.MediaThrottlingFractionEvaluator;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public class MediaSpout extends ECloudSpout {

    private ThrottlingAttributeGenerator generator;

    public MediaSpout(String topologyName, String topic, KafkaSpoutConfig<String, DpsRecord> kafkaSpoutConfig,
                      String hosts, int port, String keyspaceName, String userName, String password) {
        super(topologyName, topic, kafkaSpoutConfig, hosts, port, keyspaceName, userName, password);
    }

    protected void performThrottling(DpsTask dpsTask, StormTaskTuple tuple) {
        int parallelizationParam = readParallelizationParam(dpsTask).orElse(Integer.MAX_VALUE);
        if (parallelizationParam == 1) {
            maxTaskPending = 1;
        } else {
            maxTaskPending = parallelizationParam * 2L;
        }
        tuple.setThrottlingAttribute(generator.generate(dpsTask.getTaskId(),
                MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(parallelizationParam)));
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        generator=new ThrottlingAttributeGenerator();
    }
}
