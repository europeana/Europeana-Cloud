package eu.europeana.cloud.common.model.dps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlTransient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Delegate;

import java.util.Date;

@XmlRootElement
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskInfo {

  public static final int UNKNOWN_EXPECTED_RECORDS_NUMBER = -1;
  private long id;
  private String topologyName;
  private Date sentTimestamp;
  private Date startTimestamp;
  private Date finishTimestamp;
  @Delegate
  private EcloudTaskProgress ecloudTaskProgress = new EcloudTaskProgress();

  private TaskInfo(long id, String topologyName, Date sentTimestamp, Date startTimestamp, Date finishTimestamp, String definition) {
    this.id = id;
    this.topologyName = topologyName;
    this.sentTimestamp = sentTimestamp;
    this.startTimestamp = startTimestamp;
    this.finishTimestamp = finishTimestamp;
    this.definition = definition;
  }

  @Builder
  public TaskInfo(long id, String topologyName, EngineTaskState state, String stateDescription,
                  Date sentTimestamp, Date startTimestamp, Date finishTimestamp,
                  int expectedRecords, int successRecords, int warningRecords, int failRecords,
                  int duplicateRecords, int unchangedRecords, int processedRecords,
                  int postProcessedRecords, int expectedPostProcessedRecords, int expectedDepublishRecords,
                  int successDepublishRecords, int failDepublishRecords, int processedDepublishRecords, String definition) {
    this(id, topologyName, sentTimestamp, startTimestamp, finishTimestamp, definition);
    setExpectedRecords(expectedRecords);
    setSuccessRecords(successRecords);
    setWarningRecords(warningRecords);
    setFailRecords(failRecords);
    setDuplicateRecords(duplicateRecords);
    setUnchangedRecords(unchangedRecords);
    setProcessedRecords(processedRecords);
    setPostProcessedRecords(postProcessedRecords);
    setExpectedPostProcessedRecords(expectedPostProcessedRecords);
    setExpectedDepublishRecords(expectedDepublishRecords);
    setSuccessDepublishRecords(successDepublishRecords);
    setFailDepublishRecords(failDepublishRecords);
    setProcessedDepublishRecords(processedDepublishRecords);
    setEngineTaskState(state);
    setEngineTaskStateInfo(stateDescription);
  }


  /**
   * Full definition of the task stored in Json format
   */
  private String definition;

  @JsonIgnore
  @XmlTransient
  public boolean isProcessedOnStorm() {
    return (getSuccessRecords() + getUnchangedRecords() + getFailRecords() + getDuplicateRecords())
            == getExpectedRecords()
            && (getSuccessDepublishRecords() + getFailDepublishRecords()
            == getExpectedDepublishRecords());
  }
}
