package eu.europeana.cloud.common.model.dps;

public enum States {
    SUCCESS,
    KILLED,
    ERROR,

    /** execution failed but it will be retried */
    ERROR_WILL_RETRY,

    /** this status is used by notification bolt when whole DPS taks is processed */
    FINISHED,

    /** harvested record waits in Kafka queue */
    QUEUED
}