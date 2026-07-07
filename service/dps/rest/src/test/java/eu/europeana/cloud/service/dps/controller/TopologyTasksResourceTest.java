package eu.europeana.cloud.service.dps.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.dps.*;
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
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
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
    private static final String PROVIDER_ID= "PROVIDER_ID";
    private static final String IMAGE_TIFF = "image/tiff";
  private static final String IMAGE_JP2 = "image/jp2";
  private static final String IC_TOPOLOGY = "ic_topology";

  private static final String OAI_PMH_REPOSITORY_END_POINT = "https://example.com/oai-pmh-repository.xml";
  private static final String HTTP_COMPRESSED_FILE_URL = "https://example.com/zipFile.zip";

  private static final String LINK_CHECKING_TOPOLOGY = "linkcheck_topology";
  public static final String SAMPLE_DATASET_METIS_ID = "sampleDS";
  public static final Set<String> SAMPLE_RECORD_LIST = Set.of("/1/item1","/1/item2");

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
        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();

        task.addParameter(SCHEMA_NAME, "edm-internal");
        setCorrectlyFormulatedOutputBatch(task);

        prepareMocks(VALIDATION_TOPOLOGY);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
        assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
  }

    @Test
    void shouldProperlySendTaskWithDataSetEntryToEnrichmentTopology() throws Exception {
      CreateDpsTaskRequest task1 = new CreateDpsTaskRequest();
      task1.setSource(BatchInfo.builder()
                               .providerId(PROVIDER_ID)
                               .batchId(DATASET_ID)
                               .build());

      task1.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
      CreateDpsTaskRequest task = task1;
        setCorrectlyFormulatedOutputBatch(task);

        prepareMocks(ENRICHMENT_TOPOLOGY);
        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

        assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
    }

    @Test
    void shouldThrowDpsWhenSendingTaskToEnrichmentTopologyWithNullDatasetId() throws Exception {
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();
      task.setSource(BatchInfo.builder()
                              .providerId(PROVIDER_ID)
                              .batchId(null)
                              .build());
        prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }

    @Test
    void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetDoesNotExist() throws Exception {
        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();
      setOutputProvider(task, DATASET_URL,null);
        when(dataSetServiceClient.datasetExists(anyString(), anyString())).thenReturn(false);
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenUnexpectedExceptionHappens() throws Exception {
        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();
      setOutputProvider(task, DATASET_URL,null);
        doThrow(MCSException.class).when(dataSetServiceClient).getDataSetRepresentationsChunk(anyString(), anyString(), anyBoolean(), anyString());
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetProviderIsNotEqualToTheProviderIdParameter()
            throws Exception {
        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();
      setOutputProvider(task, DATASET_URL,null);
      ((BatchInfo)task.getSource()).setProviderId("DIFFERENT_PROVIDER_ID");
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(
                new ResultSlice<>());
        prepareMocks(TOPOLOGY_NAME);

    ResultActions response = sendTask(task, TOPOLOGY_NAME);

    response.andExpect(status().isBadRequest());
  }


    @Test
    void shouldProperlySendTaskWithOutputDataSet() throws Exception {
      CreateDpsTaskRequest task1 = new CreateDpsTaskRequest();
      task1.setSource(BatchInfo.builder()
                               .providerId(PROVIDER_ID)
                               .batchId(DATASET_ID)
                               .build());

      task1.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
      CreateDpsTaskRequest task = task1;
        setOutputProvider(task,DATASET_URL,"exampleParamName");
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyBoolean(), anyString())).thenReturn(
                new ResultSlice<>());
        prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    assertSuccessfulRequest(response, OAI_TOPOLOGY);
  }

    @Test
    void shouldProperlySendTaskWithDataSetEntryToNormalizationTopology() throws Exception {
      CreateDpsTaskRequest task1 = new CreateDpsTaskRequest();
      task1.setSource(BatchInfo.builder()
                               .providerId(PROVIDER_ID)
                               .batchId(DATASET_ID)
                               .build());

      task1.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
      CreateDpsTaskRequest task = task1;
        setCorrectlyFormulatedOutputBatch(task);
        prepareMocks(NORMALIZATION_TOPOLOGY);
        ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
    }

    @Test
    void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingRequiredParameter()
            throws Exception {

        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();
      task.setResultsProvider(PROVIDER_ID);

      prepareMocks(VALIDATION_TOPOLOGY);
        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }

  @Test
    void shouldProperlySendTaskWithOaiPmhRepository() throws Exception {
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
        setCorrectlyFormulatedOutputBatch(task);
        OAIPMHHarvestingDetails harvestingDetails = new OAIPMHHarvestingDetails();
        harvestingDetails.setRepositoryUrl(TEST_RESOURCE_URL);
        harvestingDetails.setSchema("oai_dc");
        task.setSource(harvestingDetails);
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
      CreateDpsTaskRequest task = new CreateDpsTaskRequest();
      task.setSource(OAIPMHHarvestingDetails.builder().repositoryUrl(OAI_PMH_REPOSITORY_END_POINT).build());


      prepareMocks(OAI_TOPOLOGY);
        ResultActions response = sendTask(task, OAI_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldProperlySendTaskWithHTTPRepository() throws Exception {
      CreateDpsTaskRequest task = new CreateDpsTaskRequest();
      task.setSource(new HttpHarvestingDetails(HTTP_COMPRESSED_FILE_URL));

        task.addParameter(PROVIDER_ID, PROVIDER_ID);
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
      setCorrectlyFormulatedOutputBatch(task);
        prepareMocks(HTTP_TOPOLOGY);
    ResultActions response = sendTask(task, HTTP_TOPOLOGY);

    assertSuccessfulHttpTopologyRequest(response);
  }


    @Test
    void shouldThrowExceptionWhenMissingRequiredProviderIdForHttpService() throws Exception {

      CreateDpsTaskRequest task = new CreateDpsTaskRequest();
      task.setSource(new HttpHarvestingDetails(HTTP_COMPRESSED_FILE_URL));

      prepareMocks(HTTP_TOPOLOGY);
        ResultActions response = sendTask(task, HTTP_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldProperlySendTaskWithPreviewAsTargetIndexingDatabase() throws Exception {
        //given
        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
      setOutputProvider(task,DATASET_URL,REPRESENTATION_NAME);

      prepareMocks(INDEXING_TOPOLOGY);
    //when
    ResultActions response = sendTask(task, INDEXING_TOPOLOGY);

    //then
    assertSuccessfulRequest(response, INDEXING_TOPOLOGY);
  }

  @Test
    void shouldProperlySendTaskWithPublishAsTargetIndexingDatabase() throws Exception {
        //given
        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();

        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
        task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
    setOutputProvider(task, DATASET_URL ,REPRESENTATION_NAME);
    prepareMocks(INDEXING_TOPOLOGY);

    //when
    ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

    //then
    sendTaskResponse.andExpect(status().isCreated());
  }

    @Test
    void shouldThrowExceptionWhenTargetIndexingDatabaseIsMissing() throws Exception {
        //given
      CreateDpsTaskRequest task1 = new CreateDpsTaskRequest();
      task1.setSource(BatchInfo.builder()
                               .providerId(PROVIDER_ID)
                               .batchId(DATASET_ID)
                               .build());

      task1.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
      CreateDpsTaskRequest task = task1;
      setOutputProvider(task, null,null);
      prepareMocks(INDEXING_TOPOLOGY);

        //when
        ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

        //then
    sendTaskResponse.andExpect(status().isBadRequest());
  }


    @Test
    void shouldThrowExceptionWhenTargetIndexingDatabaseIsNotProper() throws Exception {
        //given
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();
      task.setSource(BatchInfo.builder()
                              .providerId(PROVIDER_ID)
                              .batchId(DATASET_ID)
                              .build());
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "wrong-value");
      setOutputProvider(task,null,"REPRESENTATION_NAME");
      String topologyName = "indexing_topology";
    prepareMocks(topologyName);

    ResultActions response = sendTask(task, topologyName);

    //then
    response.andExpect(status().isBadRequest());
  }


  @Test
    void shouldNotSubmitEmptyTask() throws Exception {

        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();
        task.addParameter(SCHEMA_NAME, "edm-internal");
    setCorrectlyFormulatedOutputBatch(task);
    prepareMocks(VALIDATION_TOPOLOGY);
        when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(0);

    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

    response.andExpect(status().isCreated());
    verifyNoInteractions(recordKafkaSubmitService);
  }


    @Test
    void shouldThrowDpsTaskValidationExceptionWhenMissingRepresentationName() throws Exception {

        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();
        task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
        task.addParameter(MIME_TYPE, IMAGE_TIFF);

        prepareMocks(IC_TOPOLOGY);
        ResultActions response = sendTask(task, IC_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }


    @Test
    void shouldThrowDpsTaskValidationExceptionOnSendTask() throws Exception {

        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();

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
                .expectedRecords(100)
                .processedRecords(100)
                .failRecords(50)
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
    void shouldProperlySendTaskWithDataSetEntryToLinkCheckTopology() throws Exception {
      CreateDpsTaskRequest task1 = new CreateDpsTaskRequest();
      task1.setSource(BatchInfo.builder()
                               .providerId(PROVIDER_ID)
                               .batchId(DATASET_ID)
                               .build());

      task1.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
      CreateDpsTaskRequest task = task1;

        prepareMocks(LINK_CHECKING_TOPOLOGY);
        ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);

        assertSuccessfulRequest(response, LINK_CHECKING_TOPOLOGY);
    }


    @Test
    void shouldThrowValidationExceptionWhenSendingTaskToLinkCheckWithNullDatasetProviderId() throws Exception {
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();
      task.setSource(BatchInfo.builder()
                              .providerId(null)
                              .batchId(DATASET_ID)
                              .build());
        prepareMocks(LINK_CHECKING_TOPOLOGY);

        ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }



  private static void setOutputProvider(CreateDpsTaskRequest task,String datasetUrl, String representationName) {


    if(datasetUrl!=null){
      try {
        DataSet set = DataSetUrlParser.parse(datasetUrl);
        task.setResultsProvider(set.getProviderId());
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }


  }

  /* Depublication */
    @Test
    void shouldSupportDepublication() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
        task.addParameter(DEPUBLICATION_REASON, "reason");

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isCreated());
    }

    @Test
    void shouldDepublicationThrowsValidationExceptionWhenTryingWithDatasetUrls() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        CreateDpsTaskRequest task = getDpsTaskWithDataSetEntry();

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldDepublicationThrowsValidationExceptionWhenTryingWithRepositoryUrls() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
      CreateDpsTaskRequest task = new CreateDpsTaskRequest();
      task.setSource(new HttpHarvestingDetails("http://xxx.yy"));

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldDepublicationThrowsValidationExceptionWhenMissingMetisDatasetParameter() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldPassValidParametersToDepublicationService() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
        task.setSource(DepublicationInfo.builder().europeanaIdsToDepublish( SAMPLE_RECORD_LIST).build());
        task.addParameter(DEPUBLICATION_REASON, "reason");

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isCreated());

        mockTaskDAOFindById(task, DEPUBLICATION_TOPOLOGY);
        startTask(task, DEPUBLICATION_TOPOLOGY);

      ArgumentCaptor<SubmitTaskParameters> captor = ArgumentCaptor.forClass(SubmitTaskParameters.class);
      verify(depublicationTaskSubmitter).submitTask(captor.capture());
      assertEquals(SAMPLE_DATASET_METIS_ID, captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID));
      assertInstanceOf(DepublicationInfo.class, captor.getValue().getTask().getSource());
      assertEquals(SAMPLE_RECORD_LIST,
          ((DepublicationInfo) captor.getValue().getTask().getSource()).getEuropeanaIdsToDepublish());
    }

    @Test
    void shouldPassParametersWhenNoRecordsSelected() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        CreateDpsTaskRequest task = new CreateDpsTaskRequest();
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
        task.addParameter(DEPUBLICATION_REASON, "reason");

        sendTask(task, DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isCreated());

        mockTaskDAOFindById(task, DEPUBLICATION_TOPOLOGY);
        startTask(task, DEPUBLICATION_TOPOLOGY);

      ArgumentCaptor<SubmitTaskParameters> captor = ArgumentCaptor.forClass(SubmitTaskParameters.class);
      verify(depublicationTaskSubmitter).submitTask(captor.capture());
      assertEquals(SAMPLE_DATASET_METIS_ID, captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID));
      assertInstanceOf(DepublicationInfo.class, captor.getValue().getTask().getSource());
      assertNull(((DepublicationInfo) captor.getValue().getTask().getSource()).getEuropeanaIdsToDepublish());
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

  private CreateDpsTaskRequest getDpsTaskWithDataSetEntry() {
    CreateDpsTaskRequest task = new CreateDpsTaskRequest();
    task.setSource(BatchInfo.builder()
                            .providerId(PROVIDER_ID)
                            .batchId(DATASET_ID)
                            .build());

    task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
    return task;
  }

  private ResultActions sendTask(CreateDpsTaskRequest task, String topologyName) throws Exception {
    return mockMvc.perform(
        post(WEB_TARGET, topologyName)
            .with(httpBasic("any string", "any string"))
            .content(asJsonString(task))
            .contentType(MediaType.APPLICATION_JSON)
    );
  }

  private ResultActions startTask(CreateDpsTaskRequest task, String topologyName) throws Exception {
    return mockMvc.perform(
        put(START_WEB_TARGET, topologyName, TASK_ID)
                              .with(httpBasic("any string", "any string"))
                              .content("")
                              .contentType(MediaType.APPLICATION_JSON)
    );
  }

  private void mockTaskDAOFindById(CreateDpsTaskRequest task, String topology) throws IOException {
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

  private void setCorrectlyFormulatedOutputBatch(CreateDpsTaskRequest task) {
    task.setResultsProvider(PROVIDER_ID);
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