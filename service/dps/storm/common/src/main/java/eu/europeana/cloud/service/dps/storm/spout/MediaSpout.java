package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.throttling.ThrottlingTupleGroupSelector;
import eu.europeana.cloud.service.dps.storm.utils.SpoutProperties;
import java.util.Map;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

public class MediaSpout extends ECloudSpout {

  private transient ThrottlingTupleGroupSelector generator;
  private final String defaultMaximumParallelization;

  public MediaSpout(String topologyName, String topic, KafkaSpoutConfig<String, DpsRecord> kafkaSpoutConfig,
       SpoutProperties spoutProperties, CassandraProperties cassandraProperties) {
    super(topologyName, topic, kafkaSpoutConfig, cassandraProperties);
    this.defaultMaximumParallelization = spoutProperties.getMaxTaskParallelism() != null ? String.valueOf(spoutProperties.getMaxTaskParallelism()) : null;
  }

  @Override
  protected void performThrottling(StormTaskTuple tuple) {
    applyDefaultMaximumParallelizationIfNotSet(tuple);

    int parallelizationParam = tuple.readParallelizationParam();
    if (parallelizationParam == 1) {
      maxTaskPending = 1;
    } else {
      maxTaskPending = parallelizationParam * 2L;
    }
    tuple.setThrottlingGroupingAttribute(generator.generateForEdmObjectProcessingBolt(tuple));
  }

  private void applyDefaultMaximumParallelizationIfNotSet(StormTaskTuple tuple) {
    if (tuple.getParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION) == null) {
      tuple.addParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION, defaultMaximumParallelization);
    }
  }


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    super.open(conf, context, collector);
    generator = new ThrottlingTupleGroupSelector();
  }
}
