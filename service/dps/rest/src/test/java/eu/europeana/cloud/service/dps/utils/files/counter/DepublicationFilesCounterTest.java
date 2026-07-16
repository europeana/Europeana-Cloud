package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DepublicationInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.METIS_DATASET_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DepublicationFilesCounterTest {

    private static final int DATASET_EXPECTED_SIZE = 500;

    @Mock
    private Indexer<FullBeanImpl> indexer;
    @Mock
    private IndexWrapper indexWrapper;

    @BeforeEach
    void initMocks() throws Exception {
        when(indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH)).thenReturn(indexer);
        when(indexer.countRecords(anyString())).thenReturn((long) DATASET_EXPECTED_SIZE);
    }

    @Test
    void shouldCountRecords() throws Exception {
        int randomNum = ThreadLocalRandom.current().nextInt(1, DATASET_EXPECTED_SIZE);

        Set<String> records = new LinkedHashSet<>();
        for (int index = 1; index <= randomNum; index++) {
            records.add("r"+index);

        }

        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(METIS_DATASET_ID, "");
        dpsTask.setSource(DepublicationInfo.builder().europeanaIdsToDepublish(records).build());

    DepublicationFilesCounter depublicationFilesCounter = new DepublicationFilesCounter(indexWrapper);
    int count = depublicationFilesCounter.getFilesCount(dpsTask);

    assertEquals(randomNum, count);
  }

    @Test
    void shouldCountEntireDataset() throws Exception {
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(METIS_DATASET_ID, "");
        dpsTask.setSource(DepublicationInfo.builder().depublishWholeDataset(true).build());
        DepublicationFilesCounter depublicationFilesCounter = new DepublicationFilesCounter(indexWrapper);
        int count = depublicationFilesCounter.getFilesCount(dpsTask);

        assertEquals(DATASET_EXPECTED_SIZE, count);
    }
}
