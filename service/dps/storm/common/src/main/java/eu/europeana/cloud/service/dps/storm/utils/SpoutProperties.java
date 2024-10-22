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
public class SpoutProperties {
  private Integer workerCount;
  private Integer maxTaskParallelism;
  private Integer defaultMaximumParallelization;
  private Integer nimbusThriftPort;
  private List<String> nimbusSeeds;
  private Integer messageTimeoutInSeconds;
  private Integer maxSpoutPending;
  private Integer spoutSleepMilliseconds;
  private Integer spoutSleepEveryNIterations;
  private String bootstrapServers;
  private Integer maxPollRecords;
  private Integer fetchMaxBytes;
  private String topics;
}
