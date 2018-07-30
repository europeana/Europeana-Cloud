package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import java.io.Serializable;

/**
 * Created by Tarek on 7/30/2018.
 */
public class MCSSpoutMessageId implements Serializable {
    private long taskId;
    private String fileURL;


    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }


    public MCSSpoutMessageId(long taskId, String fileURL) {
        this.taskId = taskId;
        this.fileURL = fileURL;
    }

    public String getFileURL() {
        return fileURL;
    }

    public void setFileURL(String identifierId) {
        this.fileURL = fileURL;
    }


}

