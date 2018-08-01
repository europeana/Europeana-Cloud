package eu.europeana.cloud.service.dps.storm.spouts.kafka;

/**
 * Created by Tarek on 8/1/2018.
 */
public class HTTPSpoutMessageId {
    private long taskId;

    public HTTPSpoutMessageId(long taskId, String localId, String mimeType) {
        this.taskId = taskId;
        this.localId = localId;
        this.mimeType = mimeType;
    }

    private String localId;
    private String mimeType;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getLocalId() {
        return localId;
    }

    public void setLocalId(String localId) {
        this.localId = localId;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }




}
