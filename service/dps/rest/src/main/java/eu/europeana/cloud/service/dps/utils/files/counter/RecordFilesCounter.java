package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.FilesUrls;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 4/6/2016. File counters inside a Record task
 */
public class RecordFilesCounter extends FilesCounter {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordFilesCounter.class);

  public int getFilesCount(DpsTask task) {
    try {
      List<String> fileUrls = ((FilesUrls) task.getInput()).getFileUrls();
      return fileUrls.size();
    } catch (Exception ex) {
      LOGGER.error("An error occurred while reading the file counts of the task{} ", task.getTaskId());
      throw new RuntimeException(ex.getMessage() + ". Submission process stopped");
    }
  }
}
