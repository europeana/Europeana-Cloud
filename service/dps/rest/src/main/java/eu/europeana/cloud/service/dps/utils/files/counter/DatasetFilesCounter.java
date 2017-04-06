package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;

/**
 * Created by Tarek on 4/6/2016.
 * File counters inside a dataset task
 */
public class DatasetFilesCounter extends FilesCounter {
    private DataSetServiceClient dataSetServiceClient;
    private RecordServiceClient recordServiceClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetFilesCounter.class);

    DatasetFilesCounter(DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient) {
        this.dataSetServiceClient = dataSetServiceClient;
        this.recordServiceClient = recordServiceClient;
    }

    public int getFilesCount(DpsTask task, String authorizationHeader) throws TaskSubmissionException {
        int size = 0;
        List<String> dataSets = task.getInputData().get(DpsTask.DATASET_URLS);
        String representationName = task.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        final String revisionName = task.getParameter(PluginParameterKeys.REVISION_NAME);
        final String revisionProvider = task.getParameter(PluginParameterKeys.REVISION_PROVIDER);
        dataSetServiceClient.useAuthorizationHeader(authorizationHeader);
        recordServiceClient.useAuthorizationHeader(authorizationHeader);
        RevisionFileCounterUtil revisionFileCounterUtil = new RevisionFileCounterUtil(dataSetServiceClient, recordServiceClient);
        for (String dataSet : dataSets) {
            try {
                UrlParser urlParser = new UrlParser(dataSet);
                if (revisionName != null && revisionProvider != null) {
                    String revisionTimestamp = task.getParameter(PluginParameterKeys.REVISION_TIMESTAMP);
                    if (revisionTimestamp != null) {
                        size += revisionFileCounterUtil.getFilesCountForSpecificRevisions(representationName, revisionName, revisionProvider, urlParser, revisionTimestamp);
                    } else {
                        size += revisionFileCounterUtil.getFilesCountForTheLatestRevisions(representationName, revisionName, revisionProvider, urlParser);
                    }
                } else {
                    RepresentationIterator iterator = dataSetServiceClient.getRepresentationIterator(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                    while (iterator.hasNext()) {
                        Representation representation = iterator.next();
                        if (representationName == null || representation.getRepresentationName().equals(representationName)) {
                            size += representation.getFiles().size();
                        }
                    }
                }
            } catch (MalformedURLException ex) {
                LOGGER.error("URL in task's dataset list is malformed. Submission terminated. Wrong entry: " + dataSet);
                throw new TaskSubmissionException("Malformed URL in task: " + dataSet + ". Submission process stopped.");
            } catch (Exception ex) {
                LOGGER.error("an exception happened !! " + ex.getMessage());
                throw new RuntimeException(ex.getMessage() + ". Submission process stopped");
            }
        }
        return size;
    }


}
