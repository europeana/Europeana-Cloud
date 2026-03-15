package eu.europeana.cloud.service.dps.storm.tuple.storm;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class StormProcessingMetadata {
    int recordAttemptNumber;
    String sentDate;
    String messageProcessingStartTimeInMs;

    public StormProcessingMetadata(int recordAttemptNumber, String sentDate, String messageProcessingStartTimeInMs) {
        this(recordAttemptNumber);
        this.sentDate = sentDate;
        this.messageProcessingStartTimeInMs = messageProcessingStartTimeInMs;
    }

    public StormProcessingMetadata(int recordAttemptNumber) {
        this.recordAttemptNumber = recordAttemptNumber;
    }
}
