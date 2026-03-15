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
    long messageProcessingStartTimeInMs;

    public StormProcessingMetadata(int recordAttemptNumber, String sentDate, long messageProcessingStartTimeInMs) {
        this.recordAttemptNumber = recordAttemptNumber;
        this.sentDate = sentDate;
        this.messageProcessingStartTimeInMs = messageProcessingStartTimeInMs;
    }

}
