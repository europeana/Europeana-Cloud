package eu.europeana.cloud.service.dps;


/**
 * Service to fetch / submit tasks
 */
public interface TaskExecutionSubmitService {

	/**
	 * Submits a task for execution.
	 * 
	 * Depending on the task-type and the task-owner,
	 * the {@link DpsTask} will be submitted to a different Storm topology
	 */
    void submitTask(DpsTask task, String topology);
    
    /**
     * @return Fetches a task from the list
     */
   // DpsTask fetchTask(String topology, long taskId);
}
