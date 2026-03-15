package eu.europeana.cloud.service.dps.storm.tuple.storm;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class TaskMetadata {

    long taskId;
    String recordUri;
    String taskName;
    int recordAttemptNumber;


    public TaskMetadata(long taskId, String recordUri, String taskName, int recordAttemptNumber) {
        this.taskId = taskId;
        this.recordUri = recordUri;
        this.taskName = taskName;
        this.recordAttemptNumber = recordAttemptNumber;
    }
}
