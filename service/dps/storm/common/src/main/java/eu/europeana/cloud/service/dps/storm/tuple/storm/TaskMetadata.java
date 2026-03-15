package eu.europeana.cloud.service.dps.storm.tuple.storm;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
@NoArgsConstructor
public class TaskMetadata implements Serializable {

    long taskId;
    String recordUri;
    String taskName;
    boolean markedAsDeleted;


    public TaskMetadata(long taskId, String recordUri, String taskName) {
        this.taskId = taskId;
        this.recordUri = recordUri;
        this.taskName = taskName;
    }

    public TaskMetadata(long taskId, String recordUri, String taskName, boolean markedAsDeleted) {
        this(taskId, recordUri, taskName);
        this.markedAsDeleted = markedAsDeleted;
    }
}
