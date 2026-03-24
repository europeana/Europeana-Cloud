package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.RecordMetadata;
import eu.europeana.cloud.service.dps.storm.tuple.common.StormProcessingMetadata;
import eu.europeana.cloud.service.dps.storm.tuple.common.TaskMetadata;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.enrichment.rest.client.report.Report;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnrichmentBoltTest {

    public static final String DEREFERENCE_URL = "https:/dereference.org";
    public static final String ENRICHMENT_ENTITY_MANAGEMENT_URL_URL = "https://entity-management-url.org";
    public static final String ENRICHMENT_ENTITY_API_URL = "https://entity-api-url.org";
    public static final String ENRICHMENT_ENTITY_API_TOKEN_ENDPOINT = "https://entity-api-url.org/token";
    public static final String ENRICHMENT_ENTITY_API_GRANT_PARAMS = "some-params";
    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "enrichmentWorker")
    private EnrichmentWorker enrichmentWorker;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";


    @InjectMocks
    private EnrichmentBolt enrichmentBolt = new EnrichmentBolt(new CassandraProperties(), DEREFERENCE_URL,
            ENRICHMENT_ENTITY_MANAGEMENT_URL_URL, ENRICHMENT_ENTITY_API_URL, ENRICHMENT_ENTITY_API_TOKEN_ENDPOINT, ENRICHMENT_ENTITY_API_GRANT_PARAMS);

    @Test
    @SuppressWarnings("unchecked")
    void enrichEdmInternalSuccessfully() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);

        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskMetadata(TASK_ID, TASK_NAME),
                new RecordMetadata(SOURCE_VERSION_URL, FILE_DATA, true),
                new StormProcessingMetadata());
        String fileContent = new String(tuple.getFileData());
        when(enrichmentWorker.process(fileContent)).thenReturn(new ProcessedResult<>("enriched file content", new HashSet<>()));
        enrichmentBolt.execute(anchorTuple, tuple);
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(0))
                .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendErrorNotificationWhenTheEnrichmentFails() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));

        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskMetadata(TASK_ID, TASK_NAME),
                new RecordMetadata(SOURCE_VERSION_URL, FILE_DATA, true),
                new StormProcessingMetadata());
        tuple.setParameters(prepareStormTaskTupleParameters());
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
        assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Number of errors that occurred during enrichment:"));
        HashSet<Report> capturedReports = (HashSet<Report>) capturedValues.get(2);
        assertTrue(capturedReports.contains(report));
    }

    private HashMap<String, String> prepareStormTaskTupleParameters() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        return parameters;
    }

    @Test
    void shouldProperlySendReportsToNotificationBoltInCaseOfErrorReports() throws IOException {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));

        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskMetadata(TASK_ID, TASK_NAME),
                new RecordMetadata(SOURCE_VERSION_URL, FILE_DATA, true),
                new StormProcessingMetadata());
        tuple.setParameters(prepareStormTaskTupleParameters());
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
        assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Number of errors that occurred during enrichment:"));
        HashSet<Report> capturedReports = (HashSet<Report>) capturedValues.get(2);
        assertTrue(capturedReports.contains(reportEnrichmentError));
        assertTrue(capturedReports.contains(reportEnrichmentWarn));
        assertTrue(capturedReports.contains(reportDereferenceWarn));
        assertFalse(capturedReports.contains(reportDereferenceIgnore));
        assertFalse(capturedReports.contains(reportEnrichmentIgnore));
    }


    @Test
    void shouldSendEmptyReportSetToNotificationBoltInCaseOfOnlyIgnoreReports() throws IOException {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));

        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskMetadata(TASK_ID, TASK_NAME),
                new RecordMetadata(SOURCE_VERSION_URL, FILE_DATA, true),
                new StormProcessingMetadata());
        tuple.setParameters(prepareStormTaskTupleParameters());
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
        HashSet<Report> capturedReports = (HashSet<Report>) ((StormProcessingMetadata) capturedValues.get(4)).getReportSet();
        assertFalse(capturedReports.contains(reportEnrichmentIgnore_0));
        assertFalse(capturedReports.contains(reportEnrichmentIgnore_1));
        assertFalse(capturedReports.contains(reportEnrichmentIgnore_2));
        assertFalse(capturedReports.contains(reportDereferenceIgnore_0));
        assertFalse(capturedReports.contains(reportDereferenceIgnore_1));
        assertFalse(capturedReports.contains(reportDereferenceIgnore_2));
        assertEquals(0, capturedReports.size());
    }

    @Test
    void shouldProperlySendReportsToNotificationBoltInCaseOfOnlyWarnReports() throws IOException {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/example1.xml"));

        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskMetadata(TASK_ID, TASK_NAME),
                new RecordMetadata(SOURCE_VERSION_URL, FILE_DATA, true),
                new StormProcessingMetadata());
        tuple.setParameters(prepareStormTaskTupleParameters());
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
        HashSet<Report> capturedReports = (HashSet<Report>) ((StormProcessingMetadata) capturedValues.get(4)).getReportSet();
        assertTrue(capturedReports.contains(reportEnrichmentWarn));
        assertTrue(capturedReports.contains(reportDereferenceWarn));
        assertFalse(capturedReports.contains(reportDereferenceIgnore));
        assertFalse(capturedReports.contains(reportEnrichmentIgnore));
    }
}
