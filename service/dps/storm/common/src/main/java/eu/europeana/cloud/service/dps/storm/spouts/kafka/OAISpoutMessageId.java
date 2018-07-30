package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import java.io.Serializable;

/**
 * Created by Tarek on 7/27/2018.
 */
public class OAISpoutMessageId implements Serializable {
    private long taskId;
    private String identifierId;

    public String getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(String schemaId) {
        this.schemaId = schemaId;
    }

    private String schemaId;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }


    public OAISpoutMessageId(long taskId, String identifierId, String schemaId) {
        this.taskId = taskId;
        this.identifierId = identifierId;
        this.schemaId = schemaId;
    }

    public String getIdentifierId() {
        return identifierId;
    }

    public void setIdentifierId(String identifierId) {
        this.identifierId = identifierId;
    }


}
