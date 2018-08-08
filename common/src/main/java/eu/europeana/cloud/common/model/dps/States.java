package eu.europeana.cloud.common.model.dps;

public enum States {
    SUCCESS,
    KILLED,
    ERROR,
    IN_PROGRESS,
    ERROR_WILL_RETRY,   //execution failed but it will be retried
    FINISHED    //this status is used by notification bolt when whole DPS taks is processed
}