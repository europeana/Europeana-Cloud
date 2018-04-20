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
            } catch (MCSException ex) {
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
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(datasetName, datasetProvider, revisionProvider, revisionName, representationName, false);
        long taskId = t.getTaskId();
        for (CloudIdAndTimestampResponse cloudIdAndTimestampResponse : cloudIdAndTimestampResponseList) {
            if (!taskStatusChecker.hasKillFlag(taskId)) {
                String responseCloudId = cloudIdAndTimestampResponse.getCloudId();
                RepresentationRevisionResponse representationRevisionResponse = recordServiceClient.getRepresentationRevision(responseCloudId, representationName, revisionName, revisionProvider, DateHelper.getUTCDateString(cloudIdAndTimestampResponse.getRevisionTimestamp()));
                Representation representation = recordServiceClient.getRepresentation(responseCloudId, representationName, representationRevisionResponse.getVersion());
                emitRepresentation(t, representationName, representation);
            } else
                break;
        }
    }

    private void handleExactRevisions(StormTaskTuple t, DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName) throws MCSException {
        List<CloudTagsResponse> cloudTagsResponses = dataSetServiceClient.getDataSetRevisions(datasetProvider, datasetName, representationName, revisionName, revisionProvider, revisionTimestamp);
        long taskId = t.getTaskId();
        for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
            if (!taskStatusChecker.hasKillFlag(taskId)) {
                String responseCloudId = cloudTagsResponse.getCloudId();
                RepresentationRevisionResponse representationRevisionResponse = recordServiceClient.getRepresentationRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp);
                Representation representation = recordServiceClient.getRepresentation(responseCloudId, representationName, representationRevisionResponse.getVersion());
                emitRepresentation(t, representationName, representation);
            } else
                break;
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
        String RepresentationsJson = new Gson().toJson(representation);
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION, RepresentationsJson);
        return stormTaskTuple;
    }
}











