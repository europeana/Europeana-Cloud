package eu.europeana.cloud.service.dps.utils.files.counter;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.METIS_DATASET_ID;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.indexing.Indexer;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DepublicationFilesCounterTest {

  private static final int DATASET_EXPECTED_SIZE = 500;

  @Mock
  private Indexer indexer;
  @Mock
  private IndexWrapper indexWrapper;

  @Before
  public void initMocks() throws Exception {
    when(indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH)).thenReturn(indexer);
    when(indexer.countRecords(anyString())).thenReturn((long) DATASET_EXPECTED_SIZE);
  }

  @Test
  public void shouldCountRecords() throws Exception {
    int randomNum = ThreadLocalRandom.current().nextInt(1, DATASET_EXPECTED_SIZE);

    StringBuilder records = new StringBuilder("r1");
    for (int index = 2; index <= randomNum; index++) {
      records.append(", r");
      records.append(index);
    }

    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(METIS_DATASET_ID, "");
    dpsTask.addParameter(RECORD_IDS_TO_DEPUBLISH, records.toString());

    DepublicationFilesCounter depublicationFilesCounter = new DepublicationFilesCounter(indexWrapper);
    int count = depublicationFilesCounter.getFilesCount(dpsTask);

    assertEquals(randomNum, count);
  }

  @Test
  public void shouldCountEntireDataset() throws Exception {
    DpsTask dpsTask = new DpsTask();
    dpsTask.addParameter(METIS_DATASET_ID, "");

    DepublicationFilesCounter depublicationFilesCounter = new DepublicationFilesCounter(indexWrapper);
    int count = depublicationFilesCounter.getFilesCount(dpsTask);

    assertEquals(DATASET_EXPECTED_SIZE, count);
  }
}
