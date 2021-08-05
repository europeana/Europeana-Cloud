package eu.europeana.cloud.common.model.dps;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
public class TaskDiagnosticInfo {
    private long id;
    private int retryCount;
    private int startedCount;
    private Instant queuedTime;
    private Instant startOnStormTime;
    private Instant lastRecordFinishedOnStormTime;
    private Instant finishOnStormTime;
    private Instant postProcessingStartTime;
}
