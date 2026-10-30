package eu.europeana.cloud.service.dps.storm.io;

import static eu.europeana.cloud.service.dps.Constants.DPS_REPRESENTATION_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.MalformedURLException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ValidationAddToDatasetBoltTest {

  private static final String FILE_URL = "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/metadataRecord/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2";
  private static final String CLOUD_ID = "BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA";
  private static final String VERSION = "c73694c0-030d-11e6-a5cb-0050568c62b8";
  private static final long TASK_ID = 12345L;
  private static final String PROVIDER = "provider";
  private static final String DATASET = "dataset";

  @Mock
  private OutputCollector outputCollector;

  @Mock
  private DataSetServiceClient dataSetServiceClient;

  @Mock
  private TupleImpl anchorTuple;

  @Captor
  private ArgumentCaptor<Values> emittedCaptor;

  @InjectMocks
  private ValidationAddToDatasetBolt bolt = new ValidationAddToDatasetBolt(new CassandraProperties(),
      "localhost:8080/mcs", "user", "pass");

  @Test
  public void shouldAssingExistingRecordToNewDataset() throws MalformedURLException, MCSException {
    CommonTaskTuple tuple = new CommonTaskTuple();
    tuple.setTaskId(TASK_ID);
    tuple.setCloudId(CLOUD_ID);
    tuple.setRecordUri(FILE_URL);
    tuple.setOutputDatasetProvider(PROVIDER);
    tuple.setOutputDatasetId(DATASET);

    bolt.execute(anchorTuple, tuple);

    verify(dataSetServiceClient).assignRepresentationVersionToDataset(PROVIDER, DATASET, CLOUD_ID,
        DPS_REPRESENTATION_NAME, VERSION);
    verify(outputCollector).emit(any(Tuple.class), emittedCaptor.capture());
    Values values = emittedCaptor.getValue();
    assertEquals(7, values.size());
    assertEquals(TASK_ID, values.get(0));
    assertEquals(FILE_URL, values.get(1));
    assertEquals(CLOUD_ID, values.get(6));
  }

}