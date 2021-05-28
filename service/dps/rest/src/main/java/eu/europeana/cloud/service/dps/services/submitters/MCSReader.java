package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.RevisionIdentifier;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.util.Date;
import java.util.List;

public class MCSReader implements AutoCloseable {

    private final DataSetServiceClient dataSetServiceClient;

    private final FileServiceClient fileServiceClient;

    private final RecordServiceClient recordServiceClient;

    public MCSReader(String mcsClientURL, String authorizationHeader) {
        dataSetServiceClient = new DataSetServiceClient(mcsClientURL, authorizationHeader);
        recordServiceClient = new RecordServiceClient(mcsClientURL, authorizationHeader);
        fileServiceClient = new FileServiceClient(mcsClientURL, authorizationHeader);
    }

    public ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(
            String representationName,
            RevisionIdentifier revision, String datasetProvider, String datasetName, String startFrom) throws MCSException {
        return RetryableMethodExecutor.executeOnRest("Error while getting Revisions from data set.", () -> {
            ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(
                    datasetProvider,
                    datasetName,
                    representationName,
                    revision.getRevisionName(),
                    revision.getRevisionProviderId(),
                    DateHelper.getISODateString(revision.getCreationTimeStamp()), startFrom, null);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
            }

            return resultSlice;

        });
    }

    public List<Representation> getRepresentationsByRevision(String representationName, String revisionName, String revisionProvider, Date revisionTimestamp, String responseCloudId) throws MCSException {
        return RetryableMethodExecutor.executeOnRest("Error while getting representation revision.", () ->
                recordServiceClient.getRepresentationsByRevision(responseCloudId, representationName, revisionName, revisionProvider, DateHelper.getISODateString(revisionTimestamp)));
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
