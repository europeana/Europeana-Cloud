package eu.europeana.cloud.service.dps;


/**
 * Service to fetch / submit tasks and progress notifications
 */
public interface DpsService {

	/**
	 * Submits a task for execution.
	 * 
	 * Depending on the task-type and the task-owner,
	 * the {@link DpsTask} will be submitted to a different Storm topology
	 */
    void submitTask(DpsTask task);
    
    /**
     * @return Fetches a task from the list
     */
    DpsTask fetchTask();
    
    /**
     * @return Amount of records that have been processed 
     * by the last bolt of a topology.
     */
    String getTaskProgress(String taskId);

    /**
     * @return Info messages for the specified task
     */
    String getTaskNotification(String taskId);
}
