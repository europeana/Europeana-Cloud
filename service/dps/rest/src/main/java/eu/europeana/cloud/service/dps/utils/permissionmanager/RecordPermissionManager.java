package eu.europeana.cloud.service.dps.utils.permissionmanager;

import eu.europeana.cloud.mcs.driver.RecordServiceClient;
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
 */
public class RecordPermissionManager extends ResourcePermissionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordPermissionManager.class);
    RecordPermissionManager(ApplicationContext context) {
        super(context);
    }

    public int grantPermissionsToTaskResources(DpsTask task, String topologyName, String topologyUserName, String authorizationHeader) throws TaskSubmissionException {

        List<String> fileUrls = task.getInputData().get(DpsTask.FILE_URLS);
        int size = fileUrls.size();
        Iterator<String> it = fileUrls.iterator();
        while (it.hasNext()) {
            String fileUrl = it.next();
            try {
                UrlParser parser = new UrlParser(fileUrl);
                grantPermissionToVersion(authorizationHeader, topologyUserName,
                        parser.getPart(UrlPart.RECORDS),
                        parser.getPart(UrlPart.REPRESENTATIONS),
                        parser.getPart(UrlPart.VERSIONS));
                LOGGER.info("Permissions granted to: {}", fileUrl);

            } catch (MalformedURLException e) {
                LOGGER.error("URL in task's file list is malformed. Submission terminated. Wrong entry: " + fileUrl);
                throw new TaskSubmissionException("Malformed URL in task: " + fileUrl + ". Submission process stopped.");
            } catch (MCSException e) {
                LOGGER.error("Error while communicating MCS", e);
                throw new TaskSubmissionException("Error while communicating MCS. " + e.getMessage() + " for: " + fileUrl + ". Submission process stopped.");
            } catch (Exception e) {
                LOGGER.error("an exception happened !! " + e.getMessage());
                throw new RuntimeException(e.getMessage() + ". Submission process stopped");
            }

        }
        return size;
    }
}
