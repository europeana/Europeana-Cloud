package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.depublish.DatasetDepublisher;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.METIS_DATASET_ID;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DepublicationFilesCounterTest {
    private static final int DATASET_EXPECTED_SIZE = 500;
    private DatasetDepublisher datasetDepublisher;


    @Before
    public void initMocks() throws  Exception {
        datasetDepublisher = mock(DatasetDepublisher.class);
        when(datasetDepublisher.getRecordsCount(any(SubmitTaskParameters.class))).thenReturn((long)DATASET_EXPECTED_SIZE);
    }

    @Test
    public void shouldCountRecords() throws Exception {
        int randomNum = ThreadLocalRandom.current().nextInt(1, DATASET_EXPECTED_SIZE);

        StringBuilder records = new StringBuilder("r1");
        for(int index = 2; index <= randomNum; index++) {
            records.append(", r");
            records.append(index);
        }

        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(METIS_DATASET_ID, "");
        dpsTask.addParameter(RECORD_IDS_TO_DEPUBLISH, records.toString());


        DepublicationFilesCounter depublicationFilesCounter = new DepublicationFilesCounter(datasetDepublisher);
        int count = depublicationFilesCounter.getFilesCount(dpsTask);

        assertEquals(randomNum, count);
    }

    @Test
    public void shouldCountEntireDataset() throws Exception {
        DpsTask dpsTask = new DpsTask();
        dpsTask.addParameter(METIS_DATASET_ID, "");

        DepublicationFilesCounter depublicationFilesCounter = new DepublicationFilesCounter(datasetDepublisher);
        int count = depublicationFilesCounter.getFilesCount(dpsTask);

        assertEquals(DATASET_EXPECTED_SIZE, count);
    }
}
