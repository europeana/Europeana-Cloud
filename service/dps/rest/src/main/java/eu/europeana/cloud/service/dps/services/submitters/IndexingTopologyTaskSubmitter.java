package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.utils.ExpectedCounters;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;

public class IndexingTopologyTaskSubmitter extends MCSTaskSubmitter {

  public IndexingTopologyTaskSubmitter(TaskStatusChecker taskStatusChecker,
      RecordSubmitService recordSubmitService, String mcsClientURL, String userName, String password) {
    super(taskStatusChecker, recordSubmitService, mcsClientURL, userName, password);
  }

  @Override
  protected ExpectedCounters submitExistingRepresentation(Representation representation, SubmitTaskParameters submitParameters)
      throws TaskDroppedException {
    if (representation.containsAnnotation(getDatabase(submitParameters).getAnnotationKey())) {
      return super.submitExistingRepresentation(representation, submitParameters);
    }else {
      return ExpectedCounters.expectZeroRecords();
    }
  }

  private TargetIndexingDatabase getDatabase(SubmitTaskParameters parameters) {
    return TargetIndexingDatabase.valueOf(
        parameters.getTaskParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE));
  }
}
