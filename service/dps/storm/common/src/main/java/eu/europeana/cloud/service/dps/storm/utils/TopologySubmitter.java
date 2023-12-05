package eu.europeana.cloud.service.dps.storm.utils;

import java.util.Map;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopologySubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologySubmitter.class);

  public static final String START_TOPOLOGY_ON_LOCAL_CLUSTER_PROPERTY = "startTopologyOnLocalCluster";
  private static final boolean START_ON_LOCAL_CLUSTER = Boolean.getBoolean(START_TOPOLOGY_ON_LOCAL_CLUSTER_PROPERTY);

  private TopologySubmitter() {
  }

  public static void submitTopology(String name, Map stormConf, StormTopology topology)
      throws Exception {
    if (START_ON_LOCAL_CLUSTER) {
      LOGGER.warn("Cause \"{}\" property is set true, topology is started in LocalCluster!!!\n" +
          "This could be use for tests only!!!\n", START_TOPOLOGY_ON_LOCAL_CLUSTER_PROPERTY);
      //Do not close LocalCluster, this would stop the topology just after several seconds.
      @SuppressWarnings("java:S2095")
      LocalCluster lc = new LocalCluster();
      lc.submitTopology(name, stormConf, topology);
    } else {
      StormSubmitter.submitTopology(name, stormConf, topology);
    }
  }

}
