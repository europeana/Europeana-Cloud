package eu.europeana.cloud.common.model.dps;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Class represents one row in processed_records db table
 */
@Builder
@Getter
@ToString
public class ProcessedRecord {
    private long taskId;
    private String srcIdentifier;
    private String dstIdentifier;
    private String topologyName;
    private RecordState state;
    private String infoText;
    private String additionalInformations;
}
