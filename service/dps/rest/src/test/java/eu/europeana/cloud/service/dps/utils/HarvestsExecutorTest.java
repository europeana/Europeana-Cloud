package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.HarvestResult;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.config.CassandraHarvestExecutorContext;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.HarvestingIterator;
import eu.europeana.metis.harvesting.file.CloseableIterator;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {CassandraHarvestExecutorContext.class})
class HarvestsExecutorTest {

    public static final String DATASET_URL = "https://xyx.abc/mcs/data-providers/prov/data-sets/dat";
    public static final String METIS_DATASET_ID = "114411";
    private static final String TOPIC = "topic_1";
    private static final Instant DATE_AFTER_FULL = Instant.ofEpochMilli(2000);
    private static final String OAI_ID_1 = "http://test.abc/oai/ag50034509234";
    private static final String OAI_ID_2 = "http://test.abc/oai/ag50034507777";

    private final OaiHarvest harvest = new OaiHarvest.Builder().createOaiHarvest();

  @Mock
  private OaiHarvester harvester;

  @Mock
  private HarvestingIterator<OaiRecordHeader, OaiRecordHeader> oaiIterator;

    @Autowired
    private HarvestsExecutor executor;

    @Autowired
    private RecordSubmitService recordSubmitService;

    @Autowired
    private TaskStatusChecker taskStatusChecker;

    private DpsTask task;
    private SubmitTaskParameters parameters;
    private List<OaiRecordHeader> harvestedHeaders;
    private AutoCloseable mocks;
    private MockedStatic<HarvesterFactory> harvesterFactoryMock;

    @BeforeEach
    void setup() {
        mocks = MockitoAnnotations.openMocks(this);
        mockMetisHarvestingLibrary();
        createNewTask();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harvesterFactoryMock != null) {
            harvesterFactoryMock.close();
        }
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    void shouldEmitAllRecordsToKafka() {
        //given
        createNewTask();
        Mockito.clearInvocations(recordSubmitService);

        task.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
        harvestedHeaders = Arrays.asList(
                new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL),
                new OaiRecordHeader(OAI_ID_2, false, DATE_AFTER_FULL)
        );

        when(taskStatusChecker.hasDroppedStatus(anyLong())).thenReturn(false);
        when(oaiIterator.getCloseableIterator()).thenReturn(getCloseableIterator(harvestedHeaders));

        //when
        executor.execute(harvest, parameters);

        //then
        verify(recordSubmitService, times(1))
                .submitRecord(argThat(hasProperty("recordId", equalTo(OAI_ID_1))), any());

        verify(recordSubmitService)
                .submitRecord(
                        argThat(samePropertyValuesAs(DpsRecord.builder()
                                .taskId(task.getTaskId())
                                .recordId(OAI_ID_1)
                                .markedAsDeleted(false)
                                .build())),
                        any()
                );
    }

    @Test
    void shouldEmitNoRecordsToKafkaForDroppedTask() {
        createNewTask();
        Mockito.clearInvocations(recordSubmitService);

        task.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "false");
        harvestedHeaders = Arrays.asList(
                new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL),
                new OaiRecordHeader(OAI_ID_2, false, DATE_AFTER_FULL)
        );

        when(taskStatusChecker.hasDroppedStatus(anyLong())).thenReturn(true);
        when(oaiIterator.getCloseableIterator()).thenReturn(getCloseableIterator(harvestedHeaders));

        //when
        HarvestResult harvestResult = executor.execute(harvest, parameters);

        //then
        assertEquals(EngineTaskState.DROPPED, harvestResult.getTaskState());
        verify(recordSubmitService, never()).submitRecord(any(), any());
  }

  private @NotNull CloseableIterator<OaiRecordHeader> getCloseableIterator(List<OaiRecordHeader> harvestedHeaders) {
    return new CloseableIterator<>() {
      private final Iterator<OaiRecordHeader> it = harvestedHeaders.iterator();

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public OaiRecordHeader next() {
        return it.next();
      }

      @Override
      public void close() {
        //Nothing to do
      }
    };
  }

  private void createNewTask() {
    task = new DpsTask();
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    parameters = SubmitTaskParameters.builder().task(task).topicName(TOPIC).build();
  }


    private void mockMetisHarvestingLibrary() {
        harvesterFactoryMock = Mockito.mockStatic(HarvesterFactory.class);
        harvesterFactoryMock.when(() -> HarvesterFactory.createOaiHarvester(any(), anyInt(), anyInt()))
                .thenReturn(harvester);

        when(harvester.harvestRecordHeaders(any(OaiHarvest.class))).thenReturn(oaiIterator);
    }
}
