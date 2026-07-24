package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.ExpectedCounters;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;

public interface TaskSubmitter {

  ExpectedCounters submitTask(SubmitTaskParameters parameters)
      throws TaskSubmissionException, InterruptedException, TaskDroppedException;
}
