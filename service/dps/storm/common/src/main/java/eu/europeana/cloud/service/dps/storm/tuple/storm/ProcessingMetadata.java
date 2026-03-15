package eu.europeana.cloud.service.dps.storm.tuple.storm;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.enrichment.rest.client.report.Report;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.Set;

@Setter
@Getter
class ProcessingMetadata {

    Revision outputRevision;

    Set<Report> reportSet;

    public ProcessingMetadata(Revision outputRevision, Set<Report> reportSet) {
        this.outputRevision = outputRevision;
        this.reportSet = reportSet;
    }

    public void addReports(Collection<Report> reports) {
        this.reportSet.addAll(reports);
    }

    public boolean hasOutputRevision() {
        return this.getOutputRevision() != null;
    }
}
