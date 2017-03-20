package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.util.List;

/**
 * Created by Tarek on 3/10/2017.
 */
public class RevisionFileCounterUtil {
    private DataSetServiceClient dataSetServiceClient;
    private RecordServiceClient recordServiceClient;

    public RevisionFileCounterUtil(DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient) {
        this.dataSetServiceClient = dataSetServiceClient;
        this.recordServiceClient = recordServiceClient;
    }

    public int getFilesCountForTheLatestRevisions(String representationName, String revisionName, String revisionProvider, UrlParser urlParser) throws MCSException {
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

    public int getFilesCountForSpecificRevisions(String representationName, String revisionName, String revisionProvider, UrlParser urlParser, String revisionTimestamp) throws MCSException {
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
