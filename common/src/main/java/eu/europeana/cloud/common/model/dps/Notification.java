package eu.europeana.cloud.common.model.dps;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

/**
 * Describes fields in <b>'notifications'</b> table
 */
@Builder
@Getter
@Setter
@ToString
public class Notification {
    private long taskId;
    private int bucketNumber;
    private int resourceNum;
    private String topologyName;
    private String resource;
    private String state;
    private String infoText;
    private Map<String, String> additionalInformation;
    private String resultResource;
}
