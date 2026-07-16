package eu.europeana.cloud.common.model.dps;

import lombok.Getter;

/**
 * Created by Tarek on 4/4/2016.
 *
 * *****************************************************************************************
 * State transitions:
 *
 * Heapy path without postprocessing
 * RECEIVED => CREATED => PROCESSING_BY_REST_APPLICATION => QUEUED => PROCESSED
 *
 * Heapy path with postprocessing
 * RECEIVED => CREATED => PROCESSING_BY_REST_APPLICATION => QUEUED => READY_FOR_POST_PROCESSING => IN_POST_PROCESSING => PROCESSED
 *
 * If task has invalid configuration:
 * RECEIVED => INVALID
 *
 * Beside that Task could be DROPPED from any state if user request it, or something was wrong - for example exception happen
 *
 */
public enum EngineTaskState {

  RECEIVED("Task request was received, and task is being validated"),
  INVALID("Task failed parameters validation"),
  CREATED("Task created, but not started yet"),

  // //////////////////////////////////////////////////////////////////////////////////////////// //
  //                        States below means that task was already started                       //
  // //////////////////////////////////////////////////////////////////////////////////////////// //

  /**
   * Task is being processed by the REST application.<br/>
   * <ol>
   * <li>For OAI topology identifiers are harvested and pushed to the kafka topic;</li>
   * <ol/>
   */
  PROCESSING_BY_REST_APPLICATION("Task is being processed by the REST application"),

  /**
   * All task's records pushed to Kafka queue and waits for topology processing.<br/> Some of the records may already be processed
   * by topology.
   */
  QUEUED("All task's records pushed to Kafka queue"),

  READY_FOR_POST_PROCESSING("Ready for post-processing after topology stage is finished"),
  IN_POST_PROCESSING("Task in post-processing"),

  /**
   * Task execution stopped because of error or was canceled by user
   */
  DROPPED("Task was dropped"),
  PROCESSED("Completely processed"),


  // //////////////////////////////////////////////////////////////////////////////////////////// //
  //  @Deprecated states currently not used in code but still present in DB for historic tasks    //
  // //////////////////////////////////////////////////////////////////////////////////////////// //

  @Deprecated CURRENTLY_PROCESSING("Currently processed by the topology"),
  @Deprecated REMOVING_FROM_SOLR_AND_MONGO("Records are being removed from Solr and Mongo"),
  @Deprecated SENT("Sent");

  @Getter
  private final String defaultMessage;

    EngineTaskState(String s) {
    this.defaultMessage = s;
  }

  /**
   * Returns information if task was properly created for example validation failed or
   * creating not finished.
   * @return true - task was not properly created.
   */
  public boolean wasNotProperlyCreated() {
    return this == RECEIVED || this == INVALID;
  }
}
