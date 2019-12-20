package eu.europeana.cloud.common.model.dps;

/**
 * States of harvested records in notification/processed_records tables
 */
public enum RecordState {
    /** Processing record ends with a success */
    SUCCESS,

    /** Processing record ends with an error */
    ERROR,

    /** harvested record waits in Kafka queue */
    QUEUED
}