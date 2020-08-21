package eu.europeana.cloud.common.model.dps;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class TaskTopicInfo {

    private long id;

    private String topologyName;

    private String state;

    private String topicName;

    private String ownerId;

}
