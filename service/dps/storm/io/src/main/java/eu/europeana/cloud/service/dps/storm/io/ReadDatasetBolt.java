package eu.europeana.cloud.service.dps.storm.io;


import com.google.gson.Gson;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;

public class ReadDatasetBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDatasetBolt.class);
    private final String ecloudMcsAddress;

    public ReadDatasetBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }


    @Override
    public void prepare() {
    }

    @Override
    public void execute(StormTaskTuple t) {
        DataSetServiceClient datasetClient = new DataSetServiceClient(ecloudMcsAddress);
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        datasetClient.useAuthorizationHeader(authorizationHeader);

        RecordServiceClient recordServiceClient = new RecordServiceClient(ecloudMcsAddress);
        recordServiceClient.useAuthorizationHeader(authorizationHeader);

        emitSingleRepresentationFromDataSet(t, datasetClient, recordServiceClient);
    }

    public void emitSingleRepresentationFromDataSet(StormTaskTuple t, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient) {
        final String dataSetUrl = t.getParameter(PluginParameterKeys.DATASET_URL);
        final String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        final String revisionName = t.getParameter(PluginParameterKeys.REVISION_NAME);
        final String revisionProvider = t.getParameter(PluginParameterKeys.REVISION_PROVIDER);
        t.getParameters().remove(PluginParameterKeys.REPRESENTATION_NAME);
        t.getParameters().remove(PluginParameterKeys.DATASET_URL);
        if (dataSetUrl != null) {
            try {
                final UrlParser urlParser = new UrlParser(dataSetUrl);
                if (urlParser.isUrlToDataset()) {
                    if (revisionName != null && revisionProvider != null) {
                        String revisionTimestamp = t.getParameter(PluginParameterKeys.REVISION_TIMESTAMP);
                        if (revisionTimestamp != null) {
                            handleExactRevisions(t, dataSetServiceClient, recordServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                        } else {
                            handleLatestRevisions(t, dataSetServiceClient, recordServiceClient, representationName, revisionName, revisionProvider, urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS));
                        }
                    } else {
                        RepresentationIterator iterator = dataSetServiceClient.getRepresentationIterator(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
                        long taskId = t.getTaskId();
                        while (iterator.hasNext() && !taskStatusChecker.hasKillFlag(taskId)) {
                            Representation representation = iterator.next();
                            emitRepresentation(t, representationName, representation);
                        }
                    }
                } else {
                    LOGGER.warn("dataset url is not formulated correctly {}", dataSetUrl);
                    emitErrorNotification(t.getTaskId(), dataSetUrl, "dataset url is not formulated correctly", "");
                }
            } catch (MalformedURLException ex) {
                LOGGER.error("ReadFileBolt error: {}" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), dataSetUrl, ex.getMessage(), t.getParameters().toString());
            } catch (MCSException | DriverException ex) {
                LOGGER.error("ReadFileBolt error: {}" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), dataSetUrl, ex.getMessage(), t.getParameters().toString());
            }
        } else {
            String errorMessage = "Missing dataset URL";
            LOGGER.warn(errorMessage);
            emitErrorNotification(t.getTaskId(), dataSetUrl, errorMessage, "");
        }
    }

    private void handleLatestRevisions(StormTaskTuple t, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider) throws MCSException {
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = getLatestDataSetCloudIdByRepresentationAndRevision(dataSetServiceClient, representationName, revisionName, revisionProvider, datasetName, datasetProvider);
        long taskId = t.getTaskId();
        for (CloudIdAndTimestampResponse cloudIdAndTimestampResponse : cloudIdAndTimestampResponseList) {
            if (!taskStatusChecker.hasKillFlag(taskId)) {
                String responseCloudId = cloudIdAndTimestampResponse.getCloudId();
                RepresentationRevisionResponse representationRevisionResponse = getRepresentationRevision(recordServiceClient, representationName, revisionName, revisionProvider, DateHelper.getUTCDateString(cloudIdAndTimestampResponse.getRevisionTimestamp()), responseCloudId);
                Representation representation = getRepresentation(recordServiceClient, representationName, responseCloudId, representationRevisionResponse);
                emitRepresentation(t, representationName, representation);
            } else
                break;
        }
    }

    private List<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevision(DataSetServiceClient dataSetServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider) throws MCSException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(datasetName, datasetProvider, revisionProvider, revisionName, representationName, false);
            } catch (MCSException | DriverException e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting latest cloud Id from data set. Retries left{} ", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting latest cloud Id from data set.");
                    throw e;
                }
            }
        }
    }

    private void handleExactRevisions(StormTaskTuple t, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName) throws MCSException {
        List<CloudTagsResponse> cloudTagsResponses = getDataSetRevisions(dataSetServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, datasetProvider, datasetName);
        long taskId = t.getTaskId();
        for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
            if (!taskStatusChecker.hasKillFlag(taskId)) {
                String responseCloudId = cloudTagsResponse.getCloudId();
                RepresentationRevisionResponse representationRevisionResponse = getRepresentationRevision(recordServiceClient, representationName, revisionName, revisionProvider, revisionTimestamp, responseCloudId);
                Representation representation = getRepresentation(recordServiceClient, representationName, responseCloudId, representationRevisionResponse);
                emitRepresentation(t, representationName, representation);
            } else
                break;
        }
    }

    private Representation getRepresentation(RecordServiceClient recordServiceClient, String representationName, String responseCloudId, RepresentationRevisionResponse representationRevisionResponse) throws MCSException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.getRepresentation(responseCloudId, representationName, representationRevisionResponse.getVersion());
            } catch (MCSException | DriverException e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting Representation. Retries left{}", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting Representation.");
                    throw e;
                }
            }
        }
    }

    private RepresentationRevisionResponse getRepresentationRevision(RecordServiceClient recordServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String responseCloudId) throws MCSException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.getRepresentationRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp);
            } catch (MCSException | DriverException e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting representation revision. Retries Left{} ", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting representation revision.");
                    throw e;
                }
            }
        }
    }

    private List<CloudTagsResponse> getDataSetRevisions(DataSetServiceClient dataSetServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName) throws MCSException, DriverException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return dataSetServiceClient.getDataSetRevisions(datasetProvider, datasetName, representationName, revisionName, revisionProvider, revisionTimestamp);
            } catch (MCSException | DriverException e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting Revisions from data set.Retries Left{} ", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting Revisions from data set.");
                    throw e;
                }
            }
        }
    }

    private void emitRepresentation(StormTaskTuple t, String representationName, Representation representation) {
        if (representationName == null || representation.getRepresentationName().equals(representationName)) {
            StormTaskTuple next = buildStormTaskTuple(t, representation);
            outputCollector.emit(inputTuple, next.toStormTuple());
        }
    }

    private StormTaskTuple buildStormTaskTuple(StormTaskTuple t, Representation representation) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        String representationJson = new Gson().toJson(representation);
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION, representationJson);
        return stormTaskTuple;
    }
}











