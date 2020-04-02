package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
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
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MCSReader {
    private final static Logger LOGGER = LoggerFactory.getLogger(MCSReader.class);
    private final String authorizationHeader;
    private String mcsClientURL;

    private DataSetServiceClient dataSetServiceClient;

    private FileServiceClient fileServiceClient;

    private RecordServiceClient recordServiceClient;

    public MCSReader(String mcsClientURL,String authorizationHeader){
        this.mcsClientURL=mcsClientURL;
        this.authorizationHeader=authorizationHeader;
    }



    public void open() {
        dataSetServiceClient = new DataSetServiceClient(mcsClientURL, authorizationHeader);
        recordServiceClient = new RecordServiceClient(mcsClientURL, authorizationHeader);
        fileServiceClient = new FileServiceClient(mcsClientURL, authorizationHeader);
    }

    public void close() {
        dataSetServiceClient.close();
        recordServiceClient.close();
        fileServiceClient.close();
    }


    public ResultSlice<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevisionChunk(String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider, String startFrom) throws MCSException, DriverException {
        return Retriever.retryOnError3Times("Error while getting slice of latest cloud Id from data set.", () -> {
            ResultSlice<CloudIdAndTimestampResponse> resultSlice = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(datasetName, datasetProvider, revisionProvider, revisionName, representationName, false, startFrom);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
            }
            return resultSlice;
        });
    }


    public ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName, String startFrom) throws MCSException, DriverException {
        return Retriever.retryOnError3Times("Error while getting Revisions from data set.", () -> {
                ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(datasetProvider, datasetName, representationName,
                        revisionName, revisionProvider, revisionTimestamp, startFrom, null);
                if (resultSlice == null || resultSlice.getResults() == null) {
                    throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
                }

                return resultSlice;

        });
    }

    public List<Representation> getRepresentationsByRevision(String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String responseCloudId) throws MCSException {
        return Retriever.retryOnError3Times("Error while getting representation revision.", () ->
                recordServiceClient.getRepresentationsByRevision(responseCloudId, representationName, revisionName, revisionProvider, revisionTimestamp));
    }

    public String getFileUri(Representation representation,String fileName) {
        return fileServiceClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), fileName).toString();
    }

    public RepresentationIterator getRepresentationsOfEntireDataset(UrlParser urlParser) {
        return dataSetServiceClient.getRepresentationIterator(urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
    }
}
