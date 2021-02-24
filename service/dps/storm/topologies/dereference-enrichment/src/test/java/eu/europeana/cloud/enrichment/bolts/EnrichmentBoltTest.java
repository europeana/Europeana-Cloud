package eu.europeana.cloud.enrichment.bolts;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;

import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Created by Tarek on 1/23/2018.
 */
public class EnrichmentBoltTest {

    public static final String DEREFERENCE_URL = "https:/dereference.org";
    public static final String ENRICHMENT_URL = "https://enrichment.org";

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "enrichmentWorker")
    private EnrichmentWorker enrichmentWorker;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }


    @InjectMocks
    private EnrichmentBolt enrichmentBolt = new EnrichmentBolt(DEREFERENCE_URL, ENRICHMENT_URL);

    @Test
    public void enrichEdmInternalSuccessfully() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);

        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, new HashMap<String, String>(), null);
        String fileContent = new String(tuple.getFileData());
        when(enrichmentWorker.process(eq(fileContent))).thenReturn("enriched file content");
        enrichmentBolt.execute(anchorTuple, tuple);
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
    }

    @Test
    public void sendErrorNotificationWhenTheEnrichmentFails() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);

        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters(), null);
        String fileContent = new String(tuple.getFileData());
        String errorMessage = "Dereference or Enrichment Exception";
        given(enrichmentWorker.process(eq(fileContent))).willThrow(new DereferenceException(errorMessage,
                new Throwable()));
        enrichmentBolt.execute(anchorTuple, tuple);
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);
        Assert.assertTrue(val.get("additionalInfo").toString().contains("emote Enrichment/dereference service caused the problem!. The full error:"));
        Assert.assertTrue(val.get("additionalInfo").toString().contains(errorMessage));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        return parameters;
    }
}