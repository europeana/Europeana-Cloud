package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.storm.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MCSReader implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MCSReader.class);

    private final DataSetServiceClient dataSetServiceClient;

    private final FileServiceClient fileServiceClient;

    private final RecordServiceClient recordServiceClient;

    public MCSReader(String mcsClientURL, String authorizationHeader) {
        dataSetServiceClient = new DataSetServiceClient(mcsClientURL, authorizationHeader);
        recordServiceClient = new RecordServiceClient(mcsClientURL, authorizationHeader);
        fileServiceClient = new FileServiceClient(mcsClientURL, authorizationHeader);
    }

    public ResultSlice<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevisionChunk(String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider, String startFrom) throws DriverException, MCSException {
        return RetryableMethodExecutor.executeOnRest("Error while getting slice of latest cloud Id from data set.", () -> {
            ResultSlice<CloudIdAndTimestampResponse> resultSlice = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(datasetName, datasetProvider, revisionProvider, revisionName, representationName, false, startFrom);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
            }
            return resultSlice;
        });
    }


    public ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(
            String representationName,
            Revision revision, String datasetProvider, String datasetName, String startFrom) throws DriverException, MCSException {
        return RetryableMethodExecutor.executeOnRest("Error while getting Revisions from data set.", () -> {
            ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(
                    datasetProvider,
                    datasetName,
                    representationName,
                    revision.getRevisionName(),
                    revision.getRevisionProviderId(),
                    revision.getCreationTimeStamp().toString(), startFrom, null);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
            }

            return resultSlice;

        });
    }

    public List<Representation> getRepresentationsByRevision(String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String responseCloudId) throws MCSException {
        return RetryableMethodExecutor.executeOnRest("Error while getting representation revision.", () ->
                recordServiceClient.getRepresentationsByRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp));
    }

    public RepresentationIterator getRepresentationsOfEntireDataset(UrlParser urlParser) {
        return dataSetServiceClient.getRepresentationIterator(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
    }


    public void close() {
        dataSetServiceClient.close();
        recordServiceClient.close();
        fileServiceClient.close();
    }
}
