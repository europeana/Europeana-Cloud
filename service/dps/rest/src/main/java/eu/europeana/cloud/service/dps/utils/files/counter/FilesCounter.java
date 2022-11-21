package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;

/**
 * Created by Tarek on 4/6/2016.
 */
public abstract class FilesCounter {

  /**
   * @return The number of records inside the task.
   */
  public abstract int getFilesCount(DpsTask task) throws TaskSubmissionException;
}
