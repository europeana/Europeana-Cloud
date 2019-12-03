package eu.europeana.cloud.service.dps;

import java.io.Serializable;

public class DpsRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private long taskId;
    private String recordId;

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }
}
