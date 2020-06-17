package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.service.dps.DpsTask;

public class SubmitingTaskWasKilled extends RuntimeException {

    public SubmitingTaskWasKilled(long taskId) {
        super("Task was killed while it was submiting to topology! TaskId=" + taskId);
    }

    public SubmitingTaskWasKilled(DpsTask task) {
        this(task.getTaskId());
    }

}
