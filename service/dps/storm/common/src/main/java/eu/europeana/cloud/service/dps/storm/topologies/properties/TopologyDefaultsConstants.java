package eu.europeana.cloud.service.dps.storm.topologies.properties;

public final class TopologyDefaultsConstants {

  public static final int DEFAULT_TUPLE_PROCESSING_TIME = 5 * 60; // 5min
  public static final int DEFAULT_MAX_SPOUT_PENDING = 500; //records
  public static final int DEFAULT_MAX_POLL_RECORDS = 100;
  public static final int DEFAULT_FETCH_MAX_BYTES = 20000;

  public static final int DPS_DEFAULT_MAX_ATTEMPTS = 7;
  public static final int DEFAULT_SPOUT_SLEEP_MS = 1;
  public static final int DEFAULT_SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS = 32;

  private TopologyDefaultsConstants() {
  }

}
