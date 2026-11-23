package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.BatchInfo;

public class MCSReader implements AutoCloseable {

  private final DataSetServiceClient dataSetServiceClient;

  private final FileServiceClient fileServiceClient;

  private final RecordServiceClient recordServiceClient;

  public MCSReader(String mcsClientURL, String userName, String password) {
    dataSetServiceClient = new DataSetServiceClient(mcsClientURL, userName, password);
    recordServiceClient = new RecordServiceClient(mcsClientURL, userName, password);
    fileServiceClient = new FileServiceClient(mcsClientURL, userName, password);
  }

  public RepresentationIterator getRepresentationsOfEntireDataset(BatchInfo batch) {
    return dataSetServiceClient.getRepresentationIterator(
         batch.getProviderId(), batch.getBatchId()
    );
  }


  public void close() {
    dataSetServiceClient.close();
    recordServiceClient.close();
    fileServiceClient.close();
  }
}
