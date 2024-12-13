package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.RevisionIdentifier;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.util.Date;
import java.util.List;

public class MCSReader implements AutoCloseable {

  private final DataSetServiceClient dataSetServiceClient;

  private final FileServiceClient fileServiceClient;

  private final RecordServiceClient recordServiceClient;

  public MCSReader(String mcsClientURL, String userName, String password) {
    dataSetServiceClient = new DataSetServiceClient(mcsClientURL, userName, password);
    recordServiceClient = new RecordServiceClient(mcsClientURL, userName, password);
    fileServiceClient = new FileServiceClient(mcsClientURL, userName, password);
  }

  public ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(
      String representationName,
      RevisionIdentifier revision, String datasetProvider, String datasetName, String startFrom) throws MCSException {
    return RetryableMethodExecutor.executeOnRest("Error while getting Revisions from data set.", () -> {
      ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(
          datasetProvider,
          datasetName,
          representationName,
          new Revision(revision.getRevisionName(), revision.getRevisionProviderId(), revision.getCreationTimeStamp()),
          startFrom,
          null);
      if (resultSlice == null || resultSlice.getResults() == null) {
        throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
      }

      return resultSlice;

    });
  }

  public List<RepresentationRevisionResponse> getRevisionsForTheRepresentation(String representationName, String revisionName,
      String revisionProvider, Date revisionTimestamp,
      String responseCloudId) throws MCSException {
    return RetryableMethodExecutor.executeOnRest("Error while getting representation revision.", () ->
        recordServiceClient.getRepresentationRawRevisions(responseCloudId, representationName,
            new Revision(revisionName, revisionProvider, revisionTimestamp)));
  }

  public RepresentationIterator getRepresentationsOfEntireDataset(UrlParser urlParser) {
    return dataSetServiceClient.getRepresentationIterator(
        urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS)
    );
  }


  public void close() {
    dataSetServiceClient.close();
    recordServiceClient.close();
    fileServiceClient.close();
  }
}
