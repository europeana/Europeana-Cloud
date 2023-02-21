package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;

public interface TaskSubmitter {

  void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException, InterruptedException;
}
