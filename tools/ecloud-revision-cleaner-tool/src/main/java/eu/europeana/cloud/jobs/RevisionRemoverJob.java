package eu.europeana.cloud.jobs;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.data.RevisionInformation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Date;
import java.util.List;

/**
 * Created by Tarek on 7/15/2019.
 */
public class RevisionRemoverJob implements Runnable {
    static final Logger LOGGER = Logger.getLogger(RevisionRemoverJob.class);
    private DataSetServiceClient dataSetServiceClient;
    private RecordServiceClient recordServiceClient;
    private RevisionServiceClient revisionServiceClient;


    private RevisionInformation revisionInformation;
    private String startFrom;


    public RevisionRemoverJob(DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient, RevisionInformation revisionInformation, RevisionServiceClient revisionServiceClient) {
        this.dataSetServiceClient = dataSetServiceClient;
        this.recordServiceClient = recordServiceClient;
        this.revisionServiceClient = revisionServiceClient;
        this.revisionInformation = revisionInformation;
        startFrom = null;
    }

    @Override
    public void run() {
        try {
            do {
                ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(revisionInformation.getProviderId(), revisionInformation.getDataSet(), revisionInformation.getRepresentationName(),
                        revisionInformation.getRevisionName(), revisionInformation.getRevisionProvider(), revisionInformation.getRevisionProvider(), startFrom, null);
                if (resultSlice == null || resultSlice.getResults() == null) {
                    throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
                }
                List<CloudTagsResponse> cloudTagsResponses = resultSlice.getResults();
                for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
                    Representation representation = recordServiceClient.getRepresentationByRevision(cloudTagsResponse.getCloudId(), revisionInformation.getRepresentationName(), revisionInformation.getRevisionName(), revisionInformation.getRevisionProvider(), revisionInformation.getRevisionTimeStamp());
                    if (representation.getRevisions().size() == 1) {
                        removeRepresentationWithFilesAndRevisions(cloudTagsResponse, representation);
                    } else {
                        removeRevisionsOnly(representation);
                    }
                }
                startFrom = resultSlice.getNextSlice();
            }
            while (startFrom != null);

            LOGGER.info("The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider() + "_" + revisionInformation.getRevisionTimeStamp() +
                    " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId() + " was a success!");
        } catch (Exception e) {
            System.out.println("error");
            e.printStackTrace();
            LOGGER.error("The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider() + "_" + revisionInformation.getRevisionTimeStamp() +
                    " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId() + " was a failure. Please remove it again", e);
        }
    }

    private void removeRevisionsOnly(Representation representation) throws MCSException {
        DateTime utc = new DateTime(revisionInformation.getRevisionTimeStamp(), DateTimeZone.UTC);
        Date revisionDate = utc.toDate();
        for (Revision revision : representation.getRevisions()) {
            if (revisionInformation.getRevisionName().equals(revision.getRevisionName()) && revisionInformation.getRevisionProvider().equals(revision.getRevisionProviderId()) && revisionDate.getTime() == revision.getCreationTimeStamp().getTime()) {
                revisionServiceClient.deleteRevisionFromDataSet(revisionInformation.getDataSet(), revisionInformation.getProviderId(), revisionInformation.getRevisionName(), revisionInformation.getRevisionProvider(), revisionInformation.getRevisionTimeStamp(),
                        representation.getRepresentationName(), representation.getVersion(), representation.getCloudId());
            }
        }
    }

    private void removeRepresentationWithFilesAndRevisions(CloudTagsResponse cloudTagsResponse, Representation representation) throws MCSException {
        recordServiceClient.deleteRepresentation(cloudTagsResponse.getCloudId(), representation.getRepresentationName(), representation.getVersion());
    }

    void setRevisionInformation(RevisionInformation revisionInformation) {
        this.revisionInformation = revisionInformation;
    }
}
