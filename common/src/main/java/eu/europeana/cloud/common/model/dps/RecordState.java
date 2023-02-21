package eu.europeana.cloud.common.model.dps;

/**
 * States of harvested records in notification/processed_records tables
 */
public enum RecordState {
  /**
   * harvested record waits in Kafka queue
   */
  QUEUED,

  /**
   * harvested record was processed by spout and sent to downstream bolts
   */
  PROCESSED_BY_SPOUT,

  /**
   * Indicates that statistics was generated (in StatisticsBolt) for the given Record
   */
  STATS_GENERATED,

  /**
   * Processing record ends with a success
   */
  SUCCESS,

  /**
   * Processing record ends with an error
   */
  ERROR

}
