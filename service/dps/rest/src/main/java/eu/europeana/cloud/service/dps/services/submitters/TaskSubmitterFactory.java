package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.structs.SubmitTaskParameters;
import org.springframework.stereotype.Service;

@Service
public class TaskSubmitterFactory {

    private final OaiTopologyTaskSubmitter oaiTopologyTaskSubmitter;
    private final HttpTopologyTaskSubmitter httpTopologyTaskSubmitter;
    private final OtherTopologiesTaskSubmitter otherTopologiesTaskSubmitter;

    public TaskSubmitterFactory(OaiTopologyTaskSubmitter oaiTopologyTaskSubmitter,
                                HttpTopologyTaskSubmitter httpTopologyTaskSubmitter,
                                OtherTopologiesTaskSubmitter otherTopologiesTaskSubmitter) {
        this.oaiTopologyTaskSubmitter = oaiTopologyTaskSubmitter;
        this.httpTopologyTaskSubmitter = httpTopologyTaskSubmitter;
        this.otherTopologiesTaskSubmitter = otherTopologiesTaskSubmitter;
    }

    public TaskSubmitter provideTaskSubmitter(SubmitTaskParameters parameters) {
        switch (parameters.getTopologyName()) {
            case TopologiesNames.OAI_TOPOLOGY:
                return oaiTopologyTaskSubmitter;
            case TopologiesNames.HTTP_TOPOLOGY:
                return httpTopologyTaskSubmitter;
            case TopologiesNames.ENRICHMENT_TOPOLOGY:
            case TopologiesNames.INDEXING_TOPOLOGY:
            case TopologiesNames.LINKCHECK_TOPOLOGY:
            case TopologiesNames.MEDIA_TOPOLOGY:
            case TopologiesNames.NORMALIZATION_TOPOLOGY:
            case TopologiesNames.VALIDATION_TOPOLOGY:
            case TopologiesNames.XSLT_TOPOLOGY:
                return otherTopologiesTaskSubmitter;
            default:
                throw new IllegalArgumentException("Unable to find the TaskSubmitter for the given topology name: " + parameters.getTopologyName());
        }
    }
}
