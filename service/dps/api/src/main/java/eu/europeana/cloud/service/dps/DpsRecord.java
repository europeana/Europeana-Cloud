package eu.europeana.cloud.service.dps;

import java.io.Serializable;

public class DpsRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private long taskId;
    private String recordId;
    private String schema;

    public DpsRecord() {
        this(null, null);
    }

    public DpsRecord(Long taskId, String recordId) {
        this.taskId = (taskId != null) ? taskId : 0L;
        this.recordId = recordId;
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

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[taskId=" + taskId + ", recordId=" + recordId + ", schema=" + schema + "]";
    }
}