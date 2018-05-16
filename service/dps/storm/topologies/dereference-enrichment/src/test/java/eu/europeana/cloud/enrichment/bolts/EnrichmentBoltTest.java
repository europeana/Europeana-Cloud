package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 1/23/2018.
 */
@RunWith(MockitoJUnitRunner.class)
public class EnrichmentBoltTest {

    public static final String DEREFERENCE_URL = "https:/dereference.org";
    public static final String ENRICHMENT_URL = "https://enrichment.org";

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "enrichmentWorker")
    private EnrichmentWorker enrichmentWorker;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";


    @InjectMocks
    private EnrichmentBolt enrichmentBolt = new EnrichmentBolt(DEREFERENCE_URL, ENRICHMENT_URL);

    @Test
    public void enrichEdmInternalSuccessfully() throws Exception {
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, new HashMap<String, String>(), null);
        String fileContent = new String(tuple.getFileData());
        when(enrichmentWorker.process(eq(fileContent))).thenReturn("enriched file content");
        enrichmentBolt.execute(tuple);
        assertSuccessfulValidation();
    }

    @Test
    public void sendErrorNotificationWhenTheEnrichmentFails() throws Exception {
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, null, null);
        String fileContent = new String(tuple.getFileData());
        doThrow(Exception.class).when(enrichmentWorker).process(eq(fileContent));
        enrichmentBolt.execute(tuple);
        assertFailedValidation();
    }

    private void assertSuccessfulValidation() {
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(List.class));
    }

    private void assertFailedValidation() {
        Mockito.verify(outputCollector, Mockito.times(0)).emit( Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(List.class));
    }

}