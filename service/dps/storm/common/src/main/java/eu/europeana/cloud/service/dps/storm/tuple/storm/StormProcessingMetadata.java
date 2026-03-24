package eu.europeana.cloud.service.dps.storm.tuple.storm;

import eu.europeana.enrichment.rest.client.report.Report;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Setter
@Getter
@NoArgsConstructor
public class StormProcessingMetadata implements Serializable {
    int recordAttemptNumber;
    long messageProcessingStartTimeInMs;
    private String throttlingGroupingAttribute;
    Set<Report> reportSet = new HashSet<>();


    public void addReports(Collection<Report> reports) {
        this.reportSet.addAll(reports);
    }

    public StormProcessingMetadata(int recordAttemptNumber, long messageProcessingStartTimeInMs) {
        this.recordAttemptNumber = recordAttemptNumber;
        this.messageProcessingStartTimeInMs = messageProcessingStartTimeInMs;
    }

    public StormProcessingMetadata(int recordAttemptNumber, long messageProcessingStartTimeInMs, Set<Report> reportSet) {
        this(recordAttemptNumber, messageProcessingStartTimeInMs);
        this.reportSet = reportSet;
    }

}
