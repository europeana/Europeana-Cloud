package eu.europeana.cloud.service.dps.utils.permissionmanager;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
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
 * File counters inside a dataset task
 */
public class DatasetFilesCounter extends FilesCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetFilesCounter.class);

    DatasetFilesCounter(ApplicationContext context) {
        super(context);
    }

    public int getFilesCount(DpsTask task, String authorizationHeader) throws TaskSubmissionException {
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
