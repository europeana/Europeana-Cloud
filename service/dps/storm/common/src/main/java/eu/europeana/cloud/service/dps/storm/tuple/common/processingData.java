package eu.europeana.cloud.service.dps.storm.tuple.common;

import eu.europeana.enrichment.rest.client.report.Report;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Setter
@Getter
@NoArgsConstructor
public class processingData {
    int recordAttemptNumber;
    long messageProcessingStartTimeInMs;
    private String throttlingGroupingAttribute;
    Set<Report> reportSet = new HashSet<>();


    public void addReports(Collection<Report> reports) {
        this.reportSet.addAll(reports);
    }

    public processingData(int recordAttemptNumber, long messageProcessingStartTimeInMs) {
        this.recordAttemptNumber = recordAttemptNumber;
        this.messageProcessingStartTimeInMs = messageProcessingStartTimeInMs;
    }

    public processingData(int recordAttemptNumber, long messageProcessingStartTimeInMs, Set<Report> reportSet) {
        this(recordAttemptNumber, messageProcessingStartTimeInMs);
        this.reportSet = reportSet;
    }

}
