package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;

/**
 * Created by Tarek on 1/23/2018.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(EnrichmentBolt.class)
@PowerMockIgnore("javax.net.ssl.*")
public class EnrichmentBoltTest {

    public static final String DEREFERENCE_URL = "http://metis-dereference-rest-test.eanadev.org";
    public static final String ENRICHMENT_URL = "http://metis-enrichment-rest-test.eanadev.org";
    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";


    @InjectMocks
    private EnrichmentBolt enrichmentBolt = new EnrichmentBolt(DEREFERENCE_URL, ENRICHMENT_URL);

    @Before
    public void init() {
        enrichmentBolt.prepare();
    }

    @Test
    public void enrichEdmInternalSuccessfully() throws Exception {
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, new HashMap<String, String>(), null);
        enrichmentBolt.execute(tuple);
        assertSuccessfulValidation();
    }

    @Test
    public void sendErrorNotificationWhenTheEnrichmentFails() throws Exception {
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, null, null);
        enrichmentBolt.execute(tuple);
        assertFailedValidation();
    }

    private void assertSuccessfulValidation() {
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

    private void assertFailedValidation() {
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

}