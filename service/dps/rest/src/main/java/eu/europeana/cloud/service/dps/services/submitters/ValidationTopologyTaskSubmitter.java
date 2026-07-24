package eu.europeana.cloud.service.dps.services.submitters;

import static eu.europeana.cloud.common.model.RepresentationVersionAnnotation.AnnotationKey.VALID;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.dps.storm.utils.ExpectedCounters;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;

public class ValidationTopologyTaskSubmitter extends MCSTaskSubmitter {

  public ValidationTopologyTaskSubmitter(TaskStatusChecker taskStatusChecker,
      RecordSubmitService recordSubmitService, String mcsClientURL, String userName, String password) {
    super(taskStatusChecker, recordSubmitService, mcsClientURL, userName, password);
  }

  @Override
  protected ExpectedCounters submitExistingRepresentation(Representation representation, SubmitTaskParameters submitParameters)
      throws TaskDroppedException {
    if (representation.containsAnnotation(VALID)) {
      return super.submitExistingRepresentation(representation, submitParameters);
    }else {
      return ExpectedCounters.expectZeroRecords();
    }
  }

}
