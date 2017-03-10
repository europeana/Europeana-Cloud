package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
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
        for (String dataSet : dataSets) {
            try {
                UrlParser urlParser = new UrlParser(dataSet);
                if (revisionName != null && revisionProvider != null) {
                    String revisionTimestamp = task.getParameter(PluginParameterKeys.REVISION_TIMESTAMP);
                    if (revisionTimestamp != null) {
                        size += getFilesCountForSpecificRevisions(representationName, revisionName, revisionProvider, urlParser, revisionTimestamp);
                    } else {
                        size += getFilesCountForTheLatestRevisions(representationName, revisionName, revisionProvider, urlParser);
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

    private int getFilesCountForTheLatestRevisions(String representationName, String revisionName, String revisionProvider, UrlParser urlParser) throws MCSException {
        int fileCount = 0;
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS), revisionProvider, revisionName, representationName, false);
        for (CloudIdAndTimestampResponse cloudIdAndTimestampResponse : cloudIdAndTimestampResponseList) {
            String responseCloudId = cloudIdAndTimestampResponse.getCloudId();
            RepresentationRevisionResponse representationRevisionResponse = recordServiceClient.getRepresentationRevision(responseCloudId, representationName, revisionName, revisionProvider, DateHelper.getUTCDateString(cloudIdAndTimestampResponse.getRevisionTimestamp()));
            Representation representation = recordServiceClient.getRepresentation(responseCloudId, representationName, representationRevisionResponse.getVersion());
            fileCount += representation.getFiles().size();
        }
        return fileCount;
    }

    private int getFilesCountForSpecificRevisions(String representationName, String revisionName, String revisionProvider, UrlParser urlParser, String revisionTimestamp) throws MCSException {
        int fileCount = 0;
        List<CloudTagsResponse> cloudTagsResponses = dataSetServiceClient.getDataSetRevisions(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS), representationName, revisionName, revisionProvider, revisionTimestamp);
        for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
            String responseCloudId = cloudTagsResponse.getCloudId();
            RepresentationRevisionResponse representationRevisionResponse = recordServiceClient.getRepresentationRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp);
            Representation representation = recordServiceClient.getRepresentation(responseCloudId, representationName, representationRevisionResponse.getVersion());
            fileCount += representation.getFiles().size();
        }
        return fileCount;
    }
}
