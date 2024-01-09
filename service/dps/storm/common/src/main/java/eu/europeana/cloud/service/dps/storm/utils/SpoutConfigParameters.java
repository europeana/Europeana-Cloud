package eu.europeana.cloud.service.dps.storm.utils;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

/**
 * Class responsible for storing ecloud spout config parameters.
 * Some of those parameters might be null if they are not specified in topology properties.
 */
@Getter
@Builder
public class SpoutConfigParameters {
  private Integer workerCount;
  private Integer maxTaskParallelism;
  private Integer nimbusThriftPort;
  private String inputZookeeperAddress;
  private String inputZookeeperPort;
  private List<String> nimbusSeeds;
  private List<String> stormZookeeperAddress;
  private Integer messageTimeoutInSeconds;
  private String cassandraHosts;
  private Integer cassandraPort;
  private String cassandraKeyspace;
  private String cassandraUsername;
  private String cassandraSecretToken;
  private Integer maxSpoutPending;
  private Integer spoutSleepMilliseconds;
  private Integer spoutSleepEveryNIterations;
  private String bootstrapServers;
  private Integer maxPollRecords;
  private Integer fetchMaxBytes;
  private String topics;
}
