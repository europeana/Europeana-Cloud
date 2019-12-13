package eu.europeana.cloud.service.dps;

import java.io.Serializable;

public class DpsRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private long taskId;
    private String recordId;
    private String metadataPrefix;

    public DpsRecord(Long taskId, String recordId, String metadataPrefix) {
        this.taskId = (taskId != null) ? taskId : 0L;
        this.recordId = recordId;
        this.metadataPrefix = metadataPrefix;
    }

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

    public String getMetadataPrefix() {
        return metadataPrefix;
    }

    public void setMetadataPrefix(String metadataPrefix) {
        this.metadataPrefix = metadataPrefix;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[taskId=" + taskId + ", recordId=" + recordId + ", metadataPrefix=" + metadataPrefix + "]";
    }
}