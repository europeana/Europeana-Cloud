package eu.europeana.cloud.service.dps;


/**
 * Service to fetch / submit tasks
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
     * @return Pops a task from the list
     */
    DpsTask fetchAndRemove();
}
