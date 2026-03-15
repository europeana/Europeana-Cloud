package eu.europeana.cloud.service.dps.storm.tuple.storm;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TaskMetadata {

    long taskId;
    String taskName;
    int recordAttemptNumber;


    public TaskMetadata(long taskId, String taskName, int recordAttemptNumber) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.recordAttemptNumber = recordAttemptNumber;
    }
}
