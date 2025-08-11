package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.DpsTask;
import lombok.Getter;

/**
 * TaskDroppedException is thrown when a task was earlier canceled by user or dropped by system.
 * Different threads of task procession should detect such a situation and throw this exception.
 * to interrupt execution.
 */
@Getter
public class TaskDroppedException extends RuntimeException {

  private final long taskId;

  /**
   * Creates TaskDroppedException
   * @param taskId - task id
   */
  public TaskDroppedException(long taskId) {
    super("Task was dropped! Task id: " + taskId);
    this.taskId = taskId;
  }

  /**
   * Creates TaskDroppedException
   * @param task - DpsTask
   */
  public TaskDroppedException(DpsTask task) {
    this(task.getTaskId());
  }

}
