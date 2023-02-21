package eu.europeana.cloud.service.dps;

/**
 * Service to kill tasks.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public interface TaskExecutionKillService {

  /**
   * Set kill flag to task.
   *
   * @param taskId task id to kill
   */
  void killTask(long taskId, String info);


}
