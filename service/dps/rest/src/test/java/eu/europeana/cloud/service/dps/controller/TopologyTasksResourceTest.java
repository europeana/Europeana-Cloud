package eu.europeana.cloud.service.dps.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo.DatasetRevisionInfoBuilder;
import eu.europeana.cloud.service.dps.config.DPSServiceTestContext;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.services.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.services.submitters.*;
import eu.europeana.cloud.service.dps.services.validators.TaskSubmissionValidator;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.security.acls.model.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.isA;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
@WebAppConfiguration
@ContextConfiguration(classes = {DPSServiceTestContext.class, TopologyTasksResource.class, TaskSubmitterFactory.class,
        TaskSubmissionValidator.class, SubmitTaskService.class, TaskDiagnosticInfoDAO.class,
        OaiTopologyTaskSubmitter.class, HttpTopologyTaskSubmitter.class, OtherTopologiesTaskSubmitter.class,
        TaskStatusUpdater.class, TaskStatusSynchronizer.class, MCSTaskSubmitter.class, RecordSubmitService.class,
        FileURLCreator.class})
class TopologyTasksResourceTest extends AbstractResourceTest {

    /* Endpoints */
    private static final String WEB_TARGET = TopologyTasksResource.class.getAnnotation(RequestMapping.class).value()[0];
    private static final String START_WEB_TARGET = WEB_TARGET + "/{taskId}/started";
    private static final String PROGRESS_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/progress";
    private static final String KILL_TASK_WEB_TARGET = WEB_TARGET + "/{taskId}/kill";

    /* Constants */
    private static final String DATASET_URL = "http://127.0.0.1:8080/mcs/data-providers/PROVIDER_ID/data-sets/s1";
    private static final String DATASET_ID = "s1";
  private static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
  public static final String REVISION_NAME = "REVISION_NAME";
  public static final String REVISION_PROVIDER = "REVISION_PROVIDER";
    private static final String PROVIDER_ID= "PROVIDER_ID";
    private static final String IMAGE_TIFF = "image/tiff";
  private static final String IMAGE_JP2 = "image/jp2";
  private static final String IC_TOPOLOGY = "ic_topology";
  private static final String TASK_NAME = "TASK_NAME";

  private static final String OAI_PMH_REPOSITORY_END_POINT = "https://example.com/oai-pmh-repository.xml";
  private static final String HTTP_COMPRESSED_FILE_URL = "https://example.com/zipFile.zip";

  private static final String LINK_CHECKING_TOPOLOGY = "linkcheck_topology";
  public static final String SAMPLE_DATASET_METIS_ID = "sampleDS";
  public static final String SAMPLE_RECORD_LIST = "/1/item1,/1/item2";
  private static final String BATCH_ID = "batchId";

  /* Beans (or mocked beans) */
  private ApplicationContext context;
  private CassandraTaskInfoDAO taskDAO;
  private DataSetServiceClient dataSetServiceClient;
  private FileServiceClient fileServiceClient;
  private FilesCounter filesCounter;
  private FilesCounterFactory filesCounterFactory;
  private HarvestsExecutor harvestsExecutor;
  private MutableAclService mutableAclService;
  private RecordKafkaSubmitService recordKafkaSubmitService;
  private RecordServiceClient recordServiceClient;
  private TaskExecutionReportService reportService;

  public TopologyTasksResourceTest() {
    super();
  }

  @Autowired
  DepublicationTaskSubmitter depublicationTaskSubmitter;

    @BeforeEach
    @Override
    void init() throws MCSException {
        super.init();

        context = applicationContext.getBean(ApplicationContext.class);
        taskDAO = applicationContext.getBean(CassandraTaskInfoDAO.class);
        dataSetServiceClient = applicationContext.getBean(DataSetServiceClient.class);
        fileServiceClient = applicationContext.getBean(FileServiceClient.class);
        filesCounter = applicationContext.getBean(FilesCounter.class);
        filesCounterFactory = applicationContext.getBean(FilesCounterFactory.class);
        harvestsExecutor = applicationContext.getBean(HarvestsExecutor.class);
        mutableAclService = applicationContext.getBean(MutableAclService.class);
    recordKafkaSubmitService = applicationContext.getBean(RecordKafkaSubmitService.class);
    recordServiceClient = applicationContext.getBean(RecordServiceClient.class);
    reportService = applicationContext.getBean(TaskExecutionReportService.class);

    reset(
        taskDAO,
        dataSetServiceClient,
        recordKafkaSubmitService,
        reportService,
        depublicationTaskSubmitter
    );
    when(taskDAO.findById(anyLong())).thenReturn(Optional.empty());
    when(dataSetServiceClient.datasetExists(PROVIDER_ID, DATASET_ID)).thenReturn(true);
  }

    @Test
    void shouldProperlySendTaskWithDataSetEntryToValidationTopology() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();

        task.addParameter(SCHEMA_NAME, "edm-internal");
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(VALIDATION_TOPOLOGY);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
        assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
  }

    @Test
    void shouldProperlySendTaskWithDataSetEntryAndRevisionToEnrichmentTopology() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
      setCorrectlyFormulatedOutputRevision(task);
      prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);
        assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
    }

    @Test
    void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToEnrichmentTopology() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);
        setCorrectlyFormulatedOutputBatch(task);

        prepareMocks(ENRICHMENT_TOPOLOGY);
        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

        assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
    }

    @Test
    void shouldThrowDpsWhenSendingTaskToEnrichmentTopologyWithNullDatasetId() throws Exception {
        DpsTask task = new DpsTask(TASK_NAME);
      task.setInput(DatasetRevisionInfo.builder()
                                       .providerId(PROVIDER_ID)
                                       .datasetId(null)
                                       .build());
        prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToEnrichmentTopologyWithNotValidOutputRevision()
            throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task, null,null," ", REVISION_PROVIDER);
      prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }

    @Test
    void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetDoesNotExist() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task, DATASET_URL,null,REVISION_NAME, REVISION_PROVIDER);
        when(dataSetServiceClient.datasetExists(anyString(), anyString())).thenReturn(false);
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenUnexpectedExceptionHappens() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task, DATASET_URL,null,REVISION_NAME, REVISION_PROVIDER);
        doThrow(MCSException.class).when(dataSetServiceClient).getDataSetRepresentationsChunk(anyString(), anyString(), anyBoolean(), anyString());
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetProviderIsNotEqualToTheProviderIdParameter()
            throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task, DATASET_URL,null,REVISION_NAME, REVISION_PROVIDER);
      ((DatasetRevisionInfo)task.getInput()).setProviderId("DIFFERENT_PROVIDER_ID");
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(
                new ResultSlice<>());
        prepareMocks(TOPOLOGY_NAME);

    ResultActions response = sendTask(task, TOPOLOGY_NAME);

    response.andExpect(status().isBadRequest());
  }


    @Test
    void shouldProperlySendTaskWithOutputDataSet() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry("");
        setOutput(task,DATASET_URL,"exampleParamName",REVISION_NAME, REVISION_PROVIDER);
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(
                new ResultSlice<>());
        prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    assertSuccessfulRequest(response, OAI_TOPOLOGY);
  }

  @Test
    void shouldProperlySendTaskWithFileEntryToEnrichmentTopology() throws Exception {

        DpsTask task = getDpsTaskWithFileDataEntry();
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(ENRICHMENT_TOPOLOGY);
        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
  }


    @Test
    void shouldProperlySendTaskWithDataSetEntryAndRevisionToNormalizationTopology() throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();

        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(NORMALIZATION_TOPOLOGY);
        ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
    }


    @Test
    void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToNormalizationTopology() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);
        setCorrectlyFormulatedOutputBatch(task);
        prepareMocks(NORMALIZATION_TOPOLOGY);
        ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
    }

  @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToNormalizationTopologyWithNotValidOutputRevision()
            throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task, null,null,EMPTY_STRING, REVISION_PROVIDER);

      prepareMocks(NORMALIZATION_TOPOLOGY);
        ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

        response.andExpect(status().isBadRequest());
  }

  @Test
    void shouldProperlySendTaskWithFileEntryToNormalizationTopology() throws Exception {

        DpsTask task = getDpsTaskWithFileDataEntry();
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(NORMALIZATION_TOPOLOGY);
        ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

    assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
  }


    @Test
    void shouldProperlySendTaskWithFileEntryToValidationTopology() throws Exception {
        //given
        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(SCHEMA_NAME, "edm-internal");
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(VALIDATION_TOPOLOGY);
        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

    assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingRequiredParameter()
            throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
      task.setOutput(DatasetRevisionInfo.builder()
                                        .representationName(null)
                                        .revision(new Revision(REVISION_NAME, REVISION_PROVIDER))
                                        .datasetId(DATASET_ID)
                                        .providerId(PROVIDER_ID)
                                        .build());

      prepareMocks(VALIDATION_TOPOLOGY);
        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingOutputRevision() throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);

        prepareMocks(VALIDATION_TOPOLOGY);
        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision1()
            throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task, null,REPRESENTATION_NAME," ", REVISION_PROVIDER);
      prepareMocks(VALIDATION_TOPOLOGY);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
        response.andExpect(status().isBadRequest());
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision2()
            throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task,null,REPRESENTATION_NAME,null, REVISION_PROVIDER);
      prepareMocks(VALIDATION_TOPOLOGY);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
        response.andExpect(status().isBadRequest());
  }

  @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision3()
            throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);

    setOutput(task,null,REPRESENTATION_NAME,REVISION_NAME,null);

    prepareMocks(VALIDATION_TOPOLOGY);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }

  @Test
    void shouldProperlySendTaskWithOaiPmhRepository() throws Exception {
        DpsTask task = new DpsTask();
        task.setTaskName(TASK_NAME);
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
        setCorrectlyFormulatedOutputRevision(task);
        OAIPMHHarvestingDetails harvestingDetails = new OAIPMHHarvestingDetails();
        harvestingDetails.setRepositoryUrl(TEST_RESOURCE_URL);
        harvestingDetails.setSchema("oai_dc");
        task.setInput(harvestingDetails);
        when(harvestsExecutor.execute(any(OaiHarvest.class), any(SubmitTaskParameters.class))).thenReturn(
                new HarvestResult(1, EngineTaskState.PROCESSED));
        prepareMocks(OAI_TOPOLOGY);

    ResultActions response = sendTask(task, OAI_TOPOLOGY);

    assertNotNull(response);
    response.andExpect(status().isCreated());

    mockTaskDAOFindById(task, OAI_TOPOLOGY);
    startTask(task, OAI_TOPOLOGY);

    verify(harvestsExecutor).execute(any(OaiHarvest.class), any(SubmitTaskParameters.class));
    verifyNoInteractions(recordKafkaSubmitService);
  }


    @Test
    void shouldThrowExceptionWhenMissingRequiredProviderId() throws Exception {
      DpsTask task = new DpsTask();
      task.setTaskName(TASK_NAME);
      task.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(OAI_PMH_REPOSITORY_END_POINT).build());


      prepareMocks(OAI_TOPOLOGY);
        ResultActions response = sendTask(task, OAI_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldProperlySendTaskWithHTTPRepository() throws Exception {
      DpsTask task = new DpsTask();
      task.setTaskName(TASK_NAME);
      task.setInput(new HttpHarvestingDetails(HTTP_COMPRESSED_FILE_URL));

        task.addParameter(PROVIDER_ID, PROVIDER_ID);
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
      setCorrectlyFormulatedOutputRevision(task);
        prepareMocks(HTTP_TOPOLOGY);
    ResultActions response = sendTask(task, HTTP_TOPOLOGY);

    assertSuccessfulHttpTopologyRequest(response);
  }


    @Test
    void shouldThrowExceptionWhenMissingRequiredProviderIdForHttpService() throws Exception {

      DpsTask task = new DpsTask();
      task.setTaskName(TASK_NAME);
      task.setInput(new HttpHarvestingDetails(HTTP_COMPRESSED_FILE_URL));

      prepareMocks(HTTP_TOPOLOGY);
        ResultActions response = sendTask(task, HTTP_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowExceptionWhenSubmittingTaskToHttpServiceWithNotValidOutputRevision() throws Exception {
      DpsTask task = new DpsTask();
      task.setTaskName(TASK_NAME);
      task.setInput(new HttpHarvestingDetails(HTTP_COMPRESSED_FILE_URL));

      setOutput(task,null,null,REVISION_NAME,null);
      prepareMocks(HTTP_TOPOLOGY);

        ResultActions response = sendTask(task, HTTP_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }

    @Test
    void shouldProperlySendTaskWithPreviewAsTargetIndexingDatabase() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
      setOutput(task,DATASET_URL,REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER);

      prepareMocks(INDEXING_TOPOLOGY);
    //when
    ResultActions response = sendTask(task, INDEXING_TOPOLOGY);

    //then
    assertSuccessfulRequest(response, INDEXING_TOPOLOGY);
  }

  @Test
    void shouldProperlySendTaskWithPublishAsTargetIndexingDatabase() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();

        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
    setOutput(task, DATASET_URL ,REPRESENTATION_NAME,REVISION_NAME, REVISION_PROVIDER);
    prepareMocks(INDEXING_TOPOLOGY);

    //when
    ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

    //then
    sendTaskResponse.andExpect(status().isCreated());
  }


    @Test
    void shouldProperlySendTaskWithTargetIndexingDatabaseAndFileUrls() throws Exception {
        //given
        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
      setCorrectlyFormulatedOutputRevision(task);
      task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
      prepareMocks(INDEXING_TOPOLOGY);

    //when
    ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

    //then
    sendTaskResponse.andExpect(status().isCreated());
  }


    @Test
    void shouldThrowExceptionWhenTargetIndexingDatabaseIsMissing() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);
      setOutput(task, null,null, REVISION_NAME, REVISION_PROVIDER);
      prepareMocks(INDEXING_TOPOLOGY);

        //when
        ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

        //then
    sendTaskResponse.andExpect(status().isBadRequest());
  }


    @Test
    void shouldThrowExceptionWhenTargetIndexingDatabaseIsNotProper() throws Exception {
        //given
        DpsTask task = new DpsTask("indexingTask");
      task.setInput(DatasetRevisionInfo.builder()
                                       .providerId(PROVIDER_ID)
                                       .datasetId(DATASET_ID)
                                       .build());
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "wrong-value");
      setOutput(task,null,"REPRESENTATION_NAME", REVISION_NAME, REVISION_PROVIDER);
      String topologyName = "indexing_topology";
    prepareMocks(topologyName);

    ResultActions response = sendTask(task, topologyName);

    //then
    response.andExpect(status().isBadRequest());
  }


  @Test
    void shouldNotSubmitEmptyTask() throws Exception {

        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(SCHEMA_NAME, "edm-internal");
    setCorrectlyFormulatedOutputRevision(task);
    prepareMocks(VALIDATION_TOPOLOGY);
        when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(0);

    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

    response.andExpect(status().isCreated());
    verifyNoInteractions(recordKafkaSubmitService);
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenMissingRepresentationName() throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
        task.addParameter(MIME_TYPE, IMAGE_TIFF);

        prepareMocks(IC_TOPOLOGY);
        ResultActions response = sendTask(task, IC_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowDpsTaskValidationExceptionOnSendTask() throws Exception {

        DpsTask task = getDpsTaskWithFileDataEntry();

        prepareMocks(IC_TOPOLOGY);

        ResultActions response = sendTask(task, IC_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision1() throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);
        prepareCompleteParametersForIcTask(task);
      setOutput(task,null,null, EMPTY_STRING, REVISION_PROVIDER);

      prepareMocks(IC_TOPOLOGY);
        ResultActions response = sendTask(task, IC_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }




  @Test
    void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision2() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);
        prepareCompleteParametersForIcTask(task);
    setOutput(task,null,null, EMPTY_STRING, EMPTY_STRING);

    prepareMocks(IC_TOPOLOGY);
        ResultActions response = sendTask(task, IC_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }

   @Test
    void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision3() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);
        prepareCompleteParametersForIcTask(task);
     setOutput(task, null,null,null, null);

     prepareMocks(IC_TOPOLOGY);
        ResultActions response = sendTask(task, IC_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }



  @Test
    void shouldGetProgressReport() throws Exception {

        TaskInfo taskInfo = TaskInfo.builder()
                .id(TASK_ID)
                .topologyName(TOPOLOGY_NAME)
                .state(EngineTaskState.PROCESSED)
                .stateDescription(EMPTY_STRING)
                .expectedRecordsNumber(100)
                .processedRecordsCount(100)
                .processedErrorsCount(50)
                .sentTimestamp(new Date())
                                .startTimestamp(new Date())
                                .finishTimestamp(new Date())
                                .build();

    when(reportService.getTaskProgress(TASK_ID)).thenReturn(taskInfo);
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);

    ResultActions response = mockMvc.perform(get(PROGRESS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID));

    TaskInfo resultedTaskInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),
        TaskInfo.class);
    assertThat(taskInfo, is(resultedTaskInfo));
  }


    @Test
    void shouldKillTheTask() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
        String info = "Dropped by the user";

        ResultActions response = mockMvc.perform(
                post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
        );

        response.andExpect(status().isOk());
        response.andExpect(content().string("The task was killed because of " + info));
  }


    @Test
    void shouldKillTheTaskWhenPassingTheCauseOfKilling() throws Exception {
        String info = "The aggregator decided to do so";
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
        ResultActions response = mockMvc.perform(
                post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam("info", info)
        );
        response.andExpect(status().isOk());
        response.andExpect(content().string("The task was killed because of " + info));
    }


    @Test
    void killTaskShouldFailForNonExistedTopology() throws Exception {
        doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(false);

        ResultActions response = mockMvc.perform(
                post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
        );
        response.andExpect(status().isMethodNotAllowed());
    }


    @Test
    void killTaskShouldFailWhenTaskDoesNotBelongToTopology() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService)
                .checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);

        ResultActions response = mockMvc.perform(
                post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
        );
        response.andExpect(status().isMethodNotAllowed());
    }


    @Test
    void shouldThrowExceptionIfTaskIdWasNotFound() throws Exception {
        when(reportService.getTaskProgress(TASK_ID)).thenThrow(AccessDeniedOrObjectDoesNotExistException.class);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);

        ResultActions response = mockMvc.perform(
                get(PROGRESS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
        );
        response.andExpect(status().isMethodNotAllowed());
    }


    @Test
    void shouldProperlySendTaskWithDataSetEntryAndRevisionToLinkCheckTopology() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareMocks(LINK_CHECKING_TOPOLOGY);

        ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);
        assertSuccessfulRequest(response, LINK_CHECKING_TOPOLOGY);
    }


    @Test
    void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToLinkCheckTopology() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);

        prepareMocks(LINK_CHECKING_TOPOLOGY);
        ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);

        assertSuccessfulRequest(response, LINK_CHECKING_TOPOLOGY);
    }


    @Test
    void shouldThrowValidationExceptionWhenSendingTaskToLinkCheckWithNullDatasetProviderId() throws Exception {
        DpsTask task = new DpsTask(TASK_NAME);
      task.setInput(DatasetRevisionInfo.builder()
                                       .providerId(null)
                                       .datasetId(DATASET_ID)
                                       .build());
        prepareMocks(LINK_CHECKING_TOPOLOGY);

        ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowValidationExceptionWhenSendingTaskToLinkCheckTopologyWithNotValidOutputRevision() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
      setOutput(task, null,null," ", REVISION_PROVIDER);
      prepareMocks(LINK_CHECKING_TOPOLOGY);

        ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }

  private static void setOutput(DpsTask task,String datasetUrl, String representationName, String revisionName, String revisionProvider) {
    DatasetRevisionInfoBuilder builder = DatasetRevisionInfo.builder()
                                                            .representationName(representationName)
                                                            .revision(new Revision(revisionName, revisionProvider));

    if(datasetUrl!=null){
      try {
        DataSet set = DataSetUrlParser.parse(datasetUrl);
        builder.datasetId(set.getId());
        builder.providerId(set.getProviderId());
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }
    task.setOutput(builder.build());

  }

  /* Depublication */
    @Test
    void shouldSupportDepublication() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = new DpsTask(TASK_NAME);
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
        task.addParameter(DEPUBLICATION_REASON, "reason");

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isCreated());
    }

    @Test
    void shouldDepublicationThrowsValidationExceptionWhenTryingWithDatasetUrls() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = getDpsTaskWithDataSetEntry();

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldDepublicationThrowsValidationExceptionWhenTryingWithFileUrls() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = getDpsTaskWithFileDataEntry();

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldDepublicationThrowsValidationExceptionWhenTryingWithRepositoryUrls() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
      DpsTask task = new DpsTask();
      task.setTaskName(TASK_NAME);
      task.setInput(new HttpHarvestingDetails("http://xxx.yy"));

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldDepublicationThrowsValidationExceptionWhenMissingMetisDatasetParameter() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = new DpsTask(TASK_NAME);

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldPassValidParametersToDepublicationService() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = new DpsTask(TASK_NAME);
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
        task.addParameter(RECORD_IDS_TO_DEPUBLISH, SAMPLE_RECORD_LIST);
        task.addParameter(DEPUBLICATION_REASON, "reason");

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isCreated());

        mockTaskDAOFindById(task, DEPUBLICATION_TOPOLOGY);
        startTask(task, DEPUBLICATION_TOPOLOGY);

        ArgumentCaptor<SubmitTaskParameters> captor = ArgumentCaptor.forClass(SubmitTaskParameters.class);
    verify(depublicationTaskSubmitter).submitTask(captor.capture());
    assertEquals(SAMPLE_DATASET_METIS_ID, captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID));
    assertEquals(SAMPLE_RECORD_LIST, captor.getValue().getTask().getParameter(RECORD_IDS_TO_DEPUBLISH));
  }

    @Test
    void shouldPassParametersWhenNoRecordsSelected() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = new DpsTask(TASK_NAME);
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
        task.addParameter(DEPUBLICATION_REASON, "reason");

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isCreated());

        mockTaskDAOFindById(task, DEPUBLICATION_TOPOLOGY);
        startTask(task, DEPUBLICATION_TOPOLOGY);

        ArgumentCaptor<SubmitTaskParameters> captor = ArgumentCaptor.forClass(SubmitTaskParameters.class);
        verify(depublicationTaskSubmitter).submitTask(captor.capture());
    assertEquals(SAMPLE_DATASET_METIS_ID, captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID));
    assertNull(captor.getValue().getTask().getParameter(RECORD_IDS_TO_DEPUBLISH));
  }
  /* Utilities */

  private void assertSuccessfulHttpTopologyRequest(ResultActions response) throws Exception {
    assertNotNull(response);
    response.andExpect(status().isCreated());
    verifyNoInteractions(recordKafkaSubmitService);
  }

  @SuppressWarnings("unused")
  private void assertSuccessfulRequest(ResultActions response, String ignoredTopologyName) throws Exception {
    assertNotNull(response);
    response.andExpect(status().isCreated());
  }

  private DpsTask getDpsTaskWithDataSetEntry() {
    return getDpsTaskWithDataSetEntry(REPRESENTATION_NAME);
  }

  private DpsTask getDpsTaskWithDataSetEntry(String representationName) {
    DpsTask task = new DpsTask(TASK_NAME);
    task.setInput(DatasetRevisionInfo.builder()
                                     .providerId(PROVIDER_ID)
                                     .datasetId(DATASET_ID)
                                     .representationName(representationName)
                                     .revision(Revision.builder().revisionName("sampleRevisionNAme")
                                                       .revisionProviderId("sampleRevisionProvider")
                                                       .creationTimeStamp(Date.from(Instant.parse("2021-07-12T16:50:00.000Z")))
                                                       .build())
                                     .build());

    task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);



//    task.addParameter(REVISION_NAME, "OAIPMH_HARVEST");
//    task.addParameter(REVISION_PROVIDER, "metis_test5");
//    task.addParameter(REVISION_TIMESTAMP, "2018-01-31T11:33:30.842+01:00");


    return task;
  }

  private ResultActions sendTask(DpsTask task, String topologyName) throws Exception {
    return mockMvc.perform(
        post(WEB_TARGET, topologyName)
            .with(httpBasic("any string", "any string"))
            .content(asJsonString(task))
            .contentType(MediaType.APPLICATION_JSON)
    );
  }

  private ResultActions startTask(DpsTask task, String topologyName) throws Exception {
    return mockMvc.perform(
        put(START_WEB_TARGET, topologyName, TASK_ID)
                              .with(httpBasic("any string", "any string"))
                              .content("")
                              .contentType(MediaType.APPLICATION_JSON)
    );
  }

  private void mockTaskDAOFindById(DpsTask task, String topology) throws IOException {
    TaskInfo taskInfo = mock(TaskInfo.class);
      when(taskInfo.getEngineTaskState()).thenReturn(EngineTaskState.CREATED);
    when(taskInfo.getDefinition()).thenReturn(task.toJSON());
    when(taskInfo.getTopologyName()).thenReturn(topology);
    when(taskDAO.findById(anyLong())).thenReturn(Optional.of(taskInfo));
  }

  public static String asJsonString(final Object obj) {
    try {
      return new ObjectMapper().writeValueAsString(obj);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private DpsTask getDpsTaskWithFileDataEntry() {
    DpsTask task = new DpsTask(TASK_NAME);
    task.setInput(new FilesUrls(TEST_RESOURCE_URL));
    task.addParameter(METIS_DATASET_ID, "sampleDS");
    return task;
  }

  private void prepareCompleteParametersForIcTask(DpsTask task) {
    task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
    task.addParameter(MIME_TYPE, IMAGE_TIFF);
  }

  private void setCorrectlyFormulatedOutputRevision(DpsTask task) {
    task.setOutput(DatasetRevisionInfo.builder()
                                      .representationName(REPRESENTATION_NAME)
                                      .revision(new Revision(REVISION_NAME, REVISION_PROVIDER))
            .datasetId(DATASET_ID)
            .providerId(PROVIDER_ID)
                                      .build());
  }

  private void setCorrectlyFormulatedOutputBatch(DpsTask task) {
    task.setOutput(BatchInfo.builder()
                                      .representationName(REPRESENTATION_NAME)
                                      .providerId(PROVIDER_ID)
                                      .build());
  }


  private void prepareMocks(String topologyName) throws TaskSubmissionException {
    //Mock security
    mockSecurity(topologyName);
    mockECloudClients();
  }

  @SuppressWarnings("unchecked")
  private void mockSecurity(String topologyName) {
    MutableAcl mutableAcl = mock(MutableAcl.class);
    //Mock
    when(topologyManager.containsTopology(topologyName)).thenReturn(true);
    when(mutableAcl.getEntries()).thenReturn(Collections.EMPTY_LIST);
    doNothing().when(mutableAcl).insertAce(anyInt(), any(Permission.class), any(Sid.class), anyBoolean());
    when(mutableAclService.readAclById(any(ObjectIdentity.class))).thenReturn(mutableAcl);
    when(context.getBean(RecordServiceClient.class)).thenReturn(recordServiceClient);
  }

  private void mockECloudClients() throws TaskSubmissionException {
    when(context.getBean(FileServiceClient.class)).thenReturn(fileServiceClient);
    when(context.getBean(DataSetServiceClient.class)).thenReturn(dataSetServiceClient);
    when(filesCounterFactory.createFilesCounter(any(DpsTask.class), anyString())).thenReturn(filesCounter);
    when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(1);
  }

}