package eu.europeana.cloud.service.dps.utils.permissionmanager;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.net.MalformedURLException;
import java.util.List;

/**
 * Created by Tarek on 4/6/2016.
 */
public class DatasetPermissionManager extends ResourcePermissionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetPermissionManager.class);

    DatasetPermissionManager(ApplicationContext context) {
        super(context);
    }

    public int grantPermissionsToTaskResources(DpsTask task, String topologyName, String
            topologyUserName, String authorizationHeader) throws TaskSubmissionException {
        int size = 0;
        List<String> dataSets = task.getInputData().get(DpsTask.DATASET_URLS);
        String representationName = task.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        DataSetServiceClient dataSetServiceClient = context.getBean(DataSetServiceClient.class);
        for (String dataSet : dataSets) {
            try {
                UrlParser urlParser = new UrlParser(dataSet);
                List<Representation> representations = dataSetServiceClient.useAuthorizationHeader(authorizationHeader).getDataSetRepresentations(urlParser.getPart(UrlPart.DATA_PROVIDERS),
                        urlParser.getPart(UrlPart.DATA_SETS));
                for (Representation representation : representations) {
                    if (representationName == null || representation.getRepresentationName().equals(representationName)) {
                        grantPermissionToVersion(authorizationHeader, topologyUserName, representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
                        size += representation.getFiles().size();
                    }
                }
            } catch (DataSetNotExistsException ex) {
                LOGGER.warn("Provided dataset is not existed {}", dataSet);
                throw new TaskSubmissionException("Provided dataset is not existed: " + dataSet + ". Submission process stopped.");
            } catch (MalformedURLException ex) {
                LOGGER.error("URL in task's dataset list is malformed. Submission terminated. Wrong entry: " + dataSet);
                throw new TaskSubmissionException("Malformed URL in task: " + dataSet + ". Submission process stopped.");

            } catch (MCSException ex) {
                LOGGER.error("Error while communicating MCS", ex);
                throw new TaskSubmissionException("Error while communicating MCS. " + ex.getMessage() + " for: " + dataSet + ". Submission process stopped.");
            } catch (Exception ex) {
                LOGGER.error("an exception happened !! " + ex.getMessage());
                throw new RuntimeException(ex.getMessage() + ". Submission process stopped");
            }
        }
        return size;
    }
}
