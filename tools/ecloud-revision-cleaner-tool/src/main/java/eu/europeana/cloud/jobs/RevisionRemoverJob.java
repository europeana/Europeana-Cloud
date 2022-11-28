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
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.util.Date;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 7/15/2019.
 */
public class RevisionRemoverJob implements Runnable {

  static final Logger LOGGER = LoggerFactory.getLogger(RevisionRemoverJob.class);

  private DataSetServiceClient dataSetServiceClient;
  private RecordServiceClient recordServiceClient;
  private RevisionServiceClient revisionServiceClient;
  private RevisionInformation revisionInformation;

  private static final int SLEEP_TIME = 600000;

  public RevisionRemoverJob(DataSetServiceClient dataSetServiceClient, RecordServiceClient recordServiceClient,
      RevisionInformation revisionInformation, RevisionServiceClient revisionServiceClient) {
    this.dataSetServiceClient = dataSetServiceClient;
    this.recordServiceClient = recordServiceClient;
    this.revisionServiceClient = revisionServiceClient;
    this.revisionInformation = revisionInformation;
  }

  @Override
  public void run() {
    try {
      int batchNumber = 1;
      String startFrom = null;
      do {
        ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(
            revisionInformation.getProviderId(), revisionInformation.getDataSet(), revisionInformation.getRepresentationName(),
            new Revision(revisionInformation.getRevisionName(), revisionInformation.getRevisionProvider(),
                DateHelper.parseISODate(revisionInformation.getRevisionTimeStamp())),
            startFrom, null);
        if (resultSlice == null || resultSlice.getResults() == null) {
          throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
        }
        List<CloudTagsResponse> cloudTagsResponses = resultSlice.getResults();
        for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
          List<Representation> representations = recordServiceClient.getRepresentationsByRevision(
              cloudTagsResponse.getCloudId(),
              revisionInformation.getRepresentationName(),
              new Revision(revisionInformation.getRevisionName(), revisionInformation.getRevisionProvider(),
                  DateHelper.parseISODate(revisionInformation.getRevisionTimeStamp()))
          );
          for (Representation representation : representations) {
            if (representation.getRevisions().size() == 1) {
              removeRepresentationWithFilesAndRevisions(cloudTagsResponse, representation);
              break;
            } else {
              removeRevisionsOnly(representation);
            }
          }

        }
        startFrom = resultSlice.getNextSlice();
        LOGGER.info(
            "The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider()
                + "_" + revisionInformation.getRevisionTimeStamp() +
                " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId()
                + " is in progress!. This is the batch number: " + batchNumber);
        batchNumber++;
      }
      while (startFrom != null);
      LOGGER.info(
          "The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider()
              + "_" + revisionInformation.getRevisionTimeStamp() +
              " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId()
              + " was a success!");
      LOGGER.info("***********************");
    } catch (Exception e) {
      LOGGER.error(
          "The removal of revision " + revisionInformation.getRevisionName() + "_" + revisionInformation.getRevisionProvider()
              + "_" + revisionInformation.getRevisionTimeStamp() +
              " inside dataset: " + revisionInformation.getDataSet() + "_" + revisionInformation.getProviderId()
              + " was a failure. Please remove it again", e);
    }
  }

  private void removeRevisionsOnly(Representation representation) throws MCSException {
    DateTime utc = new DateTime(revisionInformation.getRevisionTimeStamp(), DateTimeZone.UTC);
    Date revisionDate = utc.toDate();
    for (Revision revision : representation.getRevisions()) {
      if (revisionInformation.getRevisionName().equals(revision.getRevisionName()) && revisionInformation.getRevisionProvider()
                                                                                                         .equals(
                                                                                                             revision.getRevisionProviderId())
          && revisionDate.getTime() == revision.getCreationTimeStamp().getTime()) {
        removeSpecificRevision(representation);
      }
    }
  }

  private void removeSpecificRevision(Representation representation) throws MCSException {
    int retries = 2;
    while (true) {
      try {
        revisionServiceClient.deleteRevision(representation.getCloudId(), representation.getRepresentationName(),
            representation.getVersion(),
            new Revision(revisionInformation.getRevisionName(), revisionInformation.getRevisionProvider(),
                DateHelper.parseISODate(revisionInformation.getRevisionTimeStamp()))
        );
        break;
      } catch (Exception e) {
        retries--;
        LOGGER.warn("Error while removing the revision." + revisionInformation.getRevisionName() + "_"
            + revisionInformation.getRevisionProvider() + "_" + revisionInformation.getRevisionTimeStamp()
            + ". Will retry after 10 minutes. Retries left: " + retries);
        waitForTheNextCall();
        if (retries <= 0) {
          throw e;
        }
      }
    }

  }

  private void removeRepresentationWithFilesAndRevisions(CloudTagsResponse cloudTagsResponse, Representation representation)
      throws MCSException {
    int retries = 2;
    while (true) {
      try {
        recordServiceClient.deleteRepresentation(cloudTagsResponse.getCloudId(), representation.getRepresentationName(),
            representation.getVersion());
        break;
      } catch (Exception e) {
        retries--;
        LOGGER.warn("Error while removing the presentation Version. will retry after 10 minutes. Retries left: " + retries);
        waitForTheNextCall();
        if (retries <= 0) {
          throw e;
        }
      }
    }
  }

  void setRevisionInformation(RevisionInformation revisionInformation) {
    this.revisionInformation = revisionInformation;
  }

  private void waitForTheNextCall() {
    try {
      Thread.sleep(SLEEP_TIME);
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
      LOGGER.error(e1.getMessage());
    }
  }
}
