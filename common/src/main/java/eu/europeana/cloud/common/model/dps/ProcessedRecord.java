package eu.europeana.cloud.common.model.dps;

import java.util.Date;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Class represents one row in processed_records db table
 */
@Builder
@Getter
@Setter
@ToString
public class ProcessedRecord {

  private long taskId;
  private String recordId;
  private int attemptNumber;
  private String dstIdentifier;
  private String topologyName;
  private RecordState state;
  private Date starTime;
  private String infoText;
  private String additionalInformations;
}
