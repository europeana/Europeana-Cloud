package eu.europeana.cloud.common.model.dps;

public enum States {
    SUCCESS,
    DROPPED,
    KILLED,
    ERROR,
    FINISHED    //this status is used by notification bolt when whole DPS taks is processed
}