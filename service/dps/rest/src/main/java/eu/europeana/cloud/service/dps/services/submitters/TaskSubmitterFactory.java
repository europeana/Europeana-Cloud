package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class TaskSubmitterFactory {

  private final OaiTopologyTaskSubmitter oaiTopologyTaskSubmitter;
  private final HttpTopologyTaskSubmitter httpTopologyTaskSubmitter;
  private final OtherTopologiesTaskSubmitter otherTopologiesTaskSubmitter;
  private final TaskSubmitter depublicationTaskSubmitter;

  public TaskSubmitterFactory(OaiTopologyTaskSubmitter oaiTopologyTaskSubmitter,
      HttpTopologyTaskSubmitter httpTopologyTaskSubmitter,
      OtherTopologiesTaskSubmitter otherTopologiesTaskSubmitter,
      @Qualifier("depublicationTaskSubmitter") TaskSubmitter depublicationTaskSubmitter) {
    this.oaiTopologyTaskSubmitter = oaiTopologyTaskSubmitter;
    this.httpTopologyTaskSubmitter = httpTopologyTaskSubmitter;
    this.otherTopologiesTaskSubmitter = otherTopologiesTaskSubmitter;
    this.depublicationTaskSubmitter = depublicationTaskSubmitter;
  }

  public TaskSubmitter provideTaskSubmitter(SubmitTaskParameters parameters) {
    switch (parameters.getTaskInfo().getTopologyName()) {
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
      case TopologiesNames.DEPUBLICATION_TOPOLOGY:
        return depublicationTaskSubmitter;
      default:
        throw new IllegalArgumentException("Unable to find the TaskSubmitter for the given topology name: "
            + parameters.getTaskInfo().getTopologyName());
    }
  }
}
