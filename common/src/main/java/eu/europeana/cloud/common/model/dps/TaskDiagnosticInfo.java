package eu.europeana.cloud.common.model.dps;

import java.time.Instant;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
public class TaskDiagnosticInfo {

  private long taskId;
  private int startedRecordsCount;
  private int recordsRetryCount;
  private Instant queuedTime;
  private Instant startOnStormTime;
  private Instant lastRecordFinishedOnStormTime;
  private Instant finishOnStormTime;
  private Instant postProcessingStartTime;
}
