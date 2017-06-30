package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

/**
 * Created by Tarek on 4/6/2016.
 * File counters inside a Record task
 */
public class RecordFilesCounter extends FilesCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordFilesCounter.class);

    public int getFilesCount(DpsTask task, String authorizationHeader) throws TaskSubmissionException {
        try {
            List<String> fileUrls = task.getInputData().get(FILE_URLS);
            int size = fileUrls.size();
            return size;
        } catch (Exception ex) {
            LOGGER.error("An error occurred while reading the file counts of the task " + task.getTaskId());
            throw new RuntimeException(ex.getMessage() + ". Submission process stopped");
        }
    }
}
