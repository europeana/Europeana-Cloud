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
    PENDING,
    /**
     * Task is being processed by the REST application.<br/>
     * <ol>
     * <li>For OAI topology identifiers are harvested and pushed to the kafka topic;</li>
     * <ol/>
     */
    PROCESSING_BY_REST_APPLICATION,

    /**
     * All task's records pushed to Kafka queue and waits for topology processing.<br/>
     * Some of the records may already be processed by topology.
     */
    QUEUED,

    SENT,
    CURRENTLY_PROCESSING,
    DROPPED,
    PROCESSED,
    REMOVING_FROM_SOLR_AND_MONGO,
    DEPUBLISHING,
    READY_FOR_POST_PROCESSING,
    IN_POST_PROCESSING
}
