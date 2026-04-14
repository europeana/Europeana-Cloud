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
  private EngineTaskProgress engineTaskProgress = new EngineTaskProgress();
  private int expectedPostProcessedRecordsNumber;

  private TaskInfo(long id, String topologyName, Date sentTimestamp, Date startTimestamp, Date finishTimestamp, String definition) {
    this.id = id;
    this.topologyName = topologyName;
    this.sentTimestamp = sentTimestamp;
    this.startTimestamp = startTimestamp;
    this.finishTimestamp = finishTimestamp;
    this.definition = definition;
  }

  @Builder
  public TaskInfo(long id, String topologyName, Date sentTimestamp, Date startTimestamp, Date finishTimestamp, String definition, int expectedRecordsNumber,
                  int processedRecordsCount, int ignoredRecordsCount, int deletedRecordsCount, int processedErrorsCount,
                  int deletedErrorsCount, int expectedPostProcessedRecordsNumber, int postProcessedRecordsCount, EngineTaskState state, String stateDescription) {
    this(id, topologyName, sentTimestamp, startTimestamp, finishTimestamp, definition);
    setExpectedRecords(expectedRecordsNumber);
    setProcessedRecords(processedRecordsCount);
    setIgnoredRecords(ignoredRecordsCount);
    setDeletedRecords(deletedRecordsCount);
    setPostProcessedRecordsCount(postProcessedRecordsCount);
    setProcessedErrors(processedErrorsCount);
    setDeletedErrors(deletedErrorsCount);
    setEngineTaskState(state);
    setEngineTaskStateInfo(stateDescription);
    this.expectedPostProcessedRecordsNumber = expectedPostProcessedRecordsNumber;
  }


  /**
   * Full definition of the task stored in Json format
   */
  private String definition;

  @JsonIgnore
  @XmlTransient
  public boolean isProcessedOnStorm() {
    return (getProcessedRecords() + getIgnoredRecords() + getDeletedRecords())
            == getExpectedRecords();
  }
}
