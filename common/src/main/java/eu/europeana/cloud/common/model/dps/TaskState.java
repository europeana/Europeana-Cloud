package eu.europeana.cloud.common.model.dps;

/**
 * Created by Tarek on 4/4/2016.
 */
public enum TaskState {
  /**
   * Task is being prepared by the REST application.
   * <ol>
   * <li>Proper permissions are granted for the task;</li>
   * <li>Number of element that has to be processed is calculated;</li>
   * <ol/>
   */
  PENDING("Task is being prepared by the REST application"),
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

  SENT("Sent"),
  CURRENTLY_PROCESSING("Currently processed by the topology"),
  DROPPED("Task was dropped"),
  PROCESSED("Completely processed"),
  REMOVING_FROM_SOLR_AND_MONGO("Records are being removed from Solr and Mongo"),
  @Deprecated //Since depublication is implemented as a topology the state is no longer used
  DEPUBLISHING("Depublishing"),
  READY_FOR_POST_PROCESSING("Ready for post-processing after topology stage is finished"),
  IN_POST_PROCESSING("Task in post-processing");

  private final String defaultMessage;

  TaskState(String s) {
    this.defaultMessage = s;
  }

  public String getDefaultMessage() {
    return defaultMessage;
  }
}
