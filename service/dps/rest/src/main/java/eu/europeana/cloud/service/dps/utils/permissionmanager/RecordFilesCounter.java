package eu.europeana.cloud.service.dps.utils.permissionmanager;

import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Tarek on 4/6/2016.
 * File counters inside a Record task
 */
public class RecordFilesCounter extends FilesCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordFilesCounter.class);

    RecordFilesCounter(ApplicationContext context) {
        super(context);
    }

    public int getFilesCount(DpsTask task, String authorizationHeader) throws TaskSubmissionException {
        try {
            List<String> fileUrls = task.getInputData().get(DpsTask.FILE_URLS);
            int size = fileUrls.size();
            return size;
        } catch (Exception ex) {
            LOGGER.error("an exception happened !! " + ex.getMessage());
            throw new RuntimeException(ex.getMessage() + ". Submission process stopped");
        }
    }
}
