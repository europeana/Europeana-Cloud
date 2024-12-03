package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.DpsTask;
import lombok.Getter;

@Getter
public class TaskDroppedException extends RuntimeException {

  private final long taskId;

  public TaskDroppedException(long taskId) {
    super("Task was dropped! Task id: " + taskId);
    this.taskId = taskId;
  }

  public TaskDroppedException(DpsTask task) {
    this(task.getTaskId());
  }

}
