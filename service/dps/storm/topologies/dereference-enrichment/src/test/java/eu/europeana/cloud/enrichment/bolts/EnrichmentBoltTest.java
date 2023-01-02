package eu.europeana.cloud.enrichment.bolts;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.enrichment.rest.client.report.Report;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
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

public class EnrichmentBoltTest {

  public static final String DEREFERENCE_URL = "https:/dereference.org";
  public static final String ENRICHMENT_ENTITY_MANAGEMENT_URL_URL = "https://entity-management-url.org";
  public static final String ENRICHMENT_ENTITY_API_URL = "https://entity-api-url.org";
  public static final String ENRICHMENT_ENTITY_API_KEY = "some-key";

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
  private EnrichmentBolt enrichmentBolt = new EnrichmentBolt(DEREFERENCE_URL, ENRICHMENT_ENTITY_MANAGEMENT_URL_URL,
      ENRICHMENT_ENTITY_API_URL, ENRICHMENT_ENTITY_API_KEY);

  @Test
  @SuppressWarnings("unchecked")
  public void enrichEdmInternalSuccessfully() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);

    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, new HashMap<>(), null);
    String fileContent = new String(tuple.getFileData());
    when(enrichmentWorker.process(fileContent)).thenReturn(new ProcessedResult<>("enriched file content", new HashSet<>()));
    enrichmentBolt.execute(anchorTuple, tuple);
    Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), Mockito.any(List.class));
    Mockito.verify(outputCollector, Mockito.times(0))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void sendErrorNotificationWhenTheEnrichmentFails() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    Fields fields = mock(Fields.class);
    when(fields.contains(anyString())).thenReturn(false);
    when(anchorTuple.getFields()).thenReturn(fields);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters(), null);
    String fileContent = new String(tuple.getFileData());
    String errorMessage = "Dereference or Enrichment Exception";
    Report report = Report.buildEnrichmentError().withMessage(errorMessage).build();
    Set<Report> reports = new HashSet<>();
    reports.add(report);
    when(enrichmentWorker.process(fileContent)).thenReturn(new ProcessedResult<>("Enrichment failed", reports));
    enrichmentBolt.execute(anchorTuple, tuple);
    Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    Values capturedValues = captor.getValue();
    var val = (Map<String, String>) capturedValues.get(1);
    Assert.assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("ENRICHMENT"));
    Assert.assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains(errorMessage));
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }
}
