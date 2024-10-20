package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.enrichment.rest.client.report.Report;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  private EnrichmentBolt enrichmentBolt = new EnrichmentBolt(new CassandraProperties(), DEREFERENCE_URL,
      ENRICHMENT_ENTITY_MANAGEMENT_URL_URL, ENRICHMENT_ENTITY_API_URL, ENRICHMENT_ENTITY_API_KEY);

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
    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters(), null);
    String fileContent = new String(tuple.getFileData());
    String errorMessage = "Dereference or Enrichment Error";
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
    Assert.assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Number of errors that occurred during enrichment:"));
    HashSet<Report> capturedReports = (HashSet<Report>) capturedValues.get(2);
    Assert.assertTrue(capturedReports.contains(report));
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }

  @Test
  public void shouldProperlySendReportsToNotificationBoltInCaseOfErrorReports() throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters(), null);
    String fileContent = new String(tuple.getFileData());
    String errorMessage = "Dereference or Enrichment Error";
    String warnMessage = "Dereference or Enrichment Warning";
    String ignoreMessage = "Dereference or Enrichment Ignore";

    Set<Report> reports = new HashSet<>();

    Report reportEnrichmentError = Report.buildEnrichmentError().withMessage(errorMessage).build();
    Report reportEnrichmentWarn = Report.buildEnrichmentWarn().withMessage(warnMessage).build();
    Report reportEnrichmentIgnore = Report.buildEnrichmentIgnore().withMessage(ignoreMessage).build();
    Report reportDereferenceWarn = Report.buildDereferenceWarn().withMessage(warnMessage).build();
    Report reportDereferenceIgnore = Report.buildDereferenceIgnore().withMessage(ignoreMessage).build();
    reports.add(reportEnrichmentError);
    reports.add(reportEnrichmentWarn);
    reports.add(reportEnrichmentIgnore);
    reports.add(reportDereferenceWarn);
    reports.add(reportDereferenceIgnore);

    when(enrichmentWorker.process(fileContent)).thenReturn(new ProcessedResult<>("Enrichment succeeded", reports));
    enrichmentBolt.execute(anchorTuple, tuple);
    Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    Values capturedValues = captor.getValue();
    var val = (Map<String, String>) capturedValues.get(1);
    Assert.assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Number of errors that occurred during enrichment:"));
    HashSet<Report> capturedReports = (HashSet<Report>) capturedValues.get(2);
    Assert.assertTrue(capturedReports.contains(reportEnrichmentError));
    Assert.assertTrue(capturedReports.contains(reportEnrichmentWarn));
    Assert.assertTrue(capturedReports.contains(reportDereferenceWarn));
    Assert.assertFalse(capturedReports.contains(reportDereferenceIgnore));
    Assert.assertFalse(capturedReports.contains(reportEnrichmentIgnore));
  }


  @Test
  public void shouldSendEmptyReportSetToNotificationBoltInCaseOfOnlyIgnoreReports() throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
            prepareStormTaskTupleParameters(), null);
    String fileContent = new String(tuple.getFileData());
    String ignoreMessage_0 = "Dereference or Enrichment Ignore_0";
    String ignoreMessage_1 = "Dereference or Enrichment Ignore_1";
    String ignoreMessage_2 = "Dereference or Enrichment Ignore_2";
    String ignoreMessage_3 = "Dereference or Enrichment Ignore_3";
    String ignoreMessage_4 = "Dereference or Enrichment Ignore_4";
    String ignoreMessage_5 = "Dereference or Enrichment Ignore_5";

    Set<Report> reports = new HashSet<>();

    Report reportEnrichmentIgnore_0 = Report.buildEnrichmentIgnore().withMessage(ignoreMessage_0).build();
    Report reportEnrichmentIgnore_1 = Report.buildEnrichmentIgnore().withMessage(ignoreMessage_1).build();
    Report reportEnrichmentIgnore_2 = Report.buildEnrichmentIgnore().withMessage(ignoreMessage_2).build();
    Report reportDereferenceIgnore_0 = Report.buildDereferenceIgnore().withMessage(ignoreMessage_3).build();
    Report reportDereferenceIgnore_1 = Report.buildDereferenceIgnore().withMessage(ignoreMessage_4).build();
    Report reportDereferenceIgnore_2 = Report.buildDereferenceIgnore().withMessage(ignoreMessage_5).build();
    reports.add(reportEnrichmentIgnore_0);
    reports.add(reportEnrichmentIgnore_1);
    reports.add(reportEnrichmentIgnore_2);
    reports.add(reportDereferenceIgnore_0);
    reports.add(reportDereferenceIgnore_1);
    reports.add(reportDereferenceIgnore_2);

    when(enrichmentWorker.process(fileContent)).thenReturn(new ProcessedResult<>("Enrichment succeeded", reports));
    enrichmentBolt.execute(anchorTuple, tuple);
    Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
    Mockito.verify(outputCollector, Mockito.times(1))
            .emit(Mockito.any(Tuple.class), captor.capture());
    Values capturedValues = captor.getValue();
    HashSet<Report> capturedReports = (HashSet<Report>) capturedValues.get(9);
    Assert.assertFalse(capturedReports.contains(reportEnrichmentIgnore_0));
    Assert.assertFalse(capturedReports.contains(reportEnrichmentIgnore_1));
    Assert.assertFalse(capturedReports.contains(reportEnrichmentIgnore_2));
    Assert.assertFalse(capturedReports.contains(reportDereferenceIgnore_0));
    Assert.assertFalse(capturedReports.contains(reportDereferenceIgnore_1));
    Assert.assertFalse(capturedReports.contains(reportDereferenceIgnore_2));
    Assert.assertEquals(0, capturedReports.size());
  }

  @Test
  public void shouldProperlySendReportsToNotificationBoltInCaseOfOnlyWarnReports() throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters(), null);
    String fileContent = new String(tuple.getFileData());
    String warnMessage = "Dereference or Enrichment Warning";
    String ignoreMessage = "Dereference or Enrichment Ignore";
    Set<Report> reports = new HashSet<>();

    Report reportEnrichmentWarn = Report.buildEnrichmentWarn().withMessage(warnMessage).build();
    Report reportEnrichmentIgnore = Report.buildEnrichmentIgnore().withMessage(ignoreMessage).build();
    Report reportDereferenceWarn = Report.buildDereferenceWarn().withMessage(warnMessage).build();
    Report reportDereferenceIgnore = Report.buildDereferenceIgnore().withMessage(ignoreMessage).build();
    reports.add(reportEnrichmentWarn);
    reports.add(reportEnrichmentIgnore);
    reports.add(reportDereferenceWarn);
    reports.add(reportDereferenceIgnore);
    when(enrichmentWorker.process(fileContent)).thenReturn(new ProcessedResult<>("Enrichment failed", reports));
    enrichmentBolt.execute(anchorTuple, tuple);
    Mockito.verify(outputCollector, Mockito.times(0))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(List.class), Mockito.any(List.class));
    Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), captor.capture());
    Values capturedValues = captor.getValue();
    HashSet<Report> capturedReports = (HashSet<Report>) capturedValues.get(9);
    Assert.assertTrue(capturedReports.contains(reportEnrichmentWarn));
    Assert.assertTrue(capturedReports.contains(reportDereferenceWarn));
    Assert.assertFalse(capturedReports.contains(reportDereferenceIgnore));
    Assert.assertFalse(capturedReports.contains(reportEnrichmentIgnore));
  }
}
