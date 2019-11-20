package eu.europeana.cloud.common.model.dps;

/**
 * Created by Tarek on 4/4/2016.
 */
public enum TaskState {
    /**
     * Task is being prepared by the REST application.<br/>
     * <p>1. Proper permissions are granted for the task;</p>
     * <p>2. Number of element that has to be processed is calculated;</p>
     */
    PENDING,
    /**
     *Task is being processed by the REST application.<br/>
     * <p>1. For OAI topology identifiers are harvested and pushed to the kafka topic;</p>
     */
    PROCESSING_BY_REST_APPLICATION,
    SENT,
    CURRENTLY_PROCESSING,
    DROPPED,
    PROCESSED,
    REMOVING_FROM_SOLR_AND_MONGO
}
