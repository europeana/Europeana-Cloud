package eu.europeana.cloud.service.dps;

/**
 * Service to kill tasks.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public interface TaskExecutionKillService 
{

    /**
     * Set kill flag to task.
     * @param topology topology name
     * @param taskId task id to kill
     */
    void killTask(String topology, long taskId);
    
    /**
     * Check kill flag.
     * @param topology topology name
     * @param taskId task id
     * @return true if provided task id has kill flag, false otherwise
     */
    Boolean hasKillFlag(String topology, long taskId);
    
    /**
     * Remove all old kill flags from topology.
     * @param topology topology name
     * @param ttl time to live (minimum old in milliseconds for delete)
     */
    void cleanOldFlags(String topology, long ttl);
    
    /**
     * Remove kill flag of the specific task.
     * @param topology topology name
     * @param taskId task id
     */
    void removeFlag(String topology, long taskId);
}
