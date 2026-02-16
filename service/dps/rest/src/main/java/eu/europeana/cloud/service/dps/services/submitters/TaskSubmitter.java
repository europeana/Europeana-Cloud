package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.net.MalformedURLException;

public interface TaskSubmitter {

  void createDateSetIfNeeded(DpsTask dpsTask) throws MCSException, MalformedURLException, TaskSubmissionException;

  void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException, InterruptedException;
}
