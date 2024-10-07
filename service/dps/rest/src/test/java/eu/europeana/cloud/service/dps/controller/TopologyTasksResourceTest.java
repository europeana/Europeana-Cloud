package eu.europeana.cloud.service.dps.controller;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.DEPUBLICATION_REASON;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.HARVEST_DATE;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.METIS_DATASET_ID;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.MIME_TYPE;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.OUTPUT_DATA_SETS;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.OUTPUT_MIME_TYPE;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.PROVIDER_ID;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.REPRESENTATION_NAME;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.REVISION_NAME;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.REVISION_PROVIDER;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.REVISION_TIMESTAMP;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.SCHEMA_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.DEPUBLICATION_TOPOLOGY;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.ENRICHMENT_TOPOLOGY;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.HTTP_TOPOLOGY;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.INDEXING_TOPOLOGY;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.NORMALIZATION_TOPOLOGY;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.OAI_TOPOLOGY;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.VALIDATION_TOPOLOGY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.HarvestResult;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.config.DPSServiceTestContext;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.services.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.services.submitters.DepublicationTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.HttpTopologyTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.MCSTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.OaiTopologyTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.OtherTopologiesTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {DPSServiceTestContext.class, TopologyTasksResource.class, TaskSubmitterFactory.class,
    TaskSubmissionValidator.class, SubmitTaskService.class, TaskDiagnosticInfoDAO.class,
    OaiTopologyTaskSubmitter.class, HttpTopologyTaskSubmitter.class, OtherTopologiesTaskSubmitter.class,
    TaskStatusUpdater.class, TaskStatusSynchronizer.class, MCSTaskSubmitter.class, RecordSubmitService.class,
    FileURLCreator.class})
public class TopologyTasksResourceTest extends AbstractResourceTest {

  /* Endpoints */
  private static final String WEB_TARGET = TopologyTasksResource.class.getAnnotation(RequestMapping.class).value()[0];
  private static final String PROGRESS_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/progress";
  private static final String KILL_TASK_WEB_TARGET = WEB_TARGET + "/{taskId}/kill";

  /* Constants */
  private static final String DATASET_URL = "http://127.0.0.1:8080/mcs/data-providers/PROVIDER_ID/data-sets/s1";
  private static final String DATASET_ID = "s1";
  private static final String IMAGE_TIFF = "image/tiff";
  private static final String IMAGE_JP2 = "image/jp2";
  private static final String IC_TOPOLOGY = "ic_topology";
  private static final String TASK_NAME = "TASK_NAME";

  private static final String OAI_PMH_REPOSITORY_END_POINT = "https://example.com/oai-pmh-repository.xml";
  private static final String HTTP_COMPRESSED_FILE_URL = "https://example.com/zipFile.zip";
  private static final String WRONG_DATA_SET_URL = "https://wrongDataSet.com";

  private static final String LINK_CHECKING_TOPOLOGY = "linkcheck_topology";
  public static final String SAMPLE_DATASET_METIS_ID = "sampleDS";
  public static final String SAMPLE_RECORD_LIST = "/1/item1,/1/item2";

  /* Beans (or mocked beans) */
  private ApplicationContext context;
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

  @Before
  @Override
  public void init() throws MCSException {
    super.init();

    context = applicationContext.getBean(ApplicationContext.class);
    var taskDAO = applicationContext.getBean(CassandraTaskInfoDAO.class);
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
  public void shouldProperlySendTaskWithDataSetEntryToValidationTopology() throws Exception {
    //given
    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    task.addParameter(SCHEMA_NAME, "edm-internal");
    setCorrectlyFormulatedOutputRevision(task);

    prepareMocks(VALIDATION_TOPOLOGY);

    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
    assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
  }

  @Test
  public void shouldProperlySendTaskWithDataSetEntryAndRevisionToEnrichmentTopology() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    prepareTaskWithRepresentationAndRevision(task);
    prepareMocks(ENRICHMENT_TOPOLOGY);

    ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);
    assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
  }

  @Test
  public void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToEnrichmentTopology() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

    prepareMocks(ENRICHMENT_TOPOLOGY);
    ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
  }

  @Test
  public void shouldThrowDpsWhenSendingTaskToEnrichmentTopologyWithWrongDataSetURL() throws Exception {
    DpsTask task = new DpsTask(TASK_NAME);
    task.addDataEntry(DATASET_URLS, List.of(WRONG_DATA_SET_URL));
    prepareMocks(ENRICHMENT_TOPOLOGY);

    ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToEnrichmentTopologyWithNotValidOutputRevision()
      throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    Revision revision = new Revision(" ", REVISION_PROVIDER);
    task.setOutputRevision(revision);
    prepareMocks(ENRICHMENT_TOPOLOGY);

    ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetURLIsMalformed() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    task.setOutputRevision(revision);
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "Malformed dataset");
    prepareMocks(TOPOLOGY_NAME);

    ResultActions response = sendTask(task, TOPOLOGY_NAME);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetDoesNotExist() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    task.setOutputRevision(revision);
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
    when(dataSetServiceClient.datasetExists(anyString(), anyString())).thenReturn(false);
    prepareMocks(TOPOLOGY_NAME);

    ResultActions response = sendTask(task, TOPOLOGY_NAME);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenUnexpectedExceptionHappens() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    task.setOutputRevision(revision);
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
    doThrow(MCSException.class).when(dataSetServiceClient).getDataSetRepresentationsChunk(anyString(), anyString(), anyString());
    prepareMocks(TOPOLOGY_NAME);

    ResultActions response = sendTask(task, TOPOLOGY_NAME);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetProviderIsNotEqualToTheProviderIdParameter()
      throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    task.setOutputRevision(revision);
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
    task.addParameter(PluginParameterKeys.PROVIDER_ID, "DIFFERENT_PROVIDER_ID");
    when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(
        new ResultSlice<>());
    prepareMocks(TOPOLOGY_NAME);

    ResultActions response = sendTask(task, TOPOLOGY_NAME);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldProperlySendTaskWithOutputDataSet() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(PluginParameterKeys.REPRESENTATION_NAME, "exampleParamName");
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    task.setOutputRevision(revision);
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
    when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(
        new ResultSlice<>());
    prepareMocks(ENRICHMENT_TOPOLOGY);

    ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    assertSuccessfulRequest(response, OAI_TOPOLOGY);
  }


  @Test
  public void shouldProperlySendTaskWithFileEntryToEnrichmentTopology() throws Exception {

    DpsTask task = getDpsTaskWithFileDataEntry();

    task.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionNAme");
    task.addParameter(PluginParameterKeys.REVISION_PROVIDER, "sampleRevisionProvider");
    task.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-07-12T16:50:00.000Z");
    setCorrectlyFormulatedOutputRevision(task);

    prepareMocks(ENRICHMENT_TOPOLOGY);
    ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

    assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
  }


  @Test
  public void shouldProperlySendTaskWithDataSetEntryAndRevisionToNormalizationTopology() throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    setCorrectlyFormulatedOutputRevision(task);

    prepareMocks(NORMALIZATION_TOPOLOGY);
    ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

    assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
  }


  @Test
  public void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToNormalizationTopology() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

    prepareMocks(NORMALIZATION_TOPOLOGY);
    ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

    assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
  }


  @Test
  public void shouldThrowDpsWhenSendingTaskToNormalizationTopologyWithWrongDataSetURL() throws Exception {

    DpsTask task = new DpsTask(TASK_NAME);
    task.addDataEntry(DATASET_URLS, Collections.singletonList(WRONG_DATA_SET_URL));

    prepareMocks(NORMALIZATION_TOPOLOGY);
    ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToNormalizationTopologyWithNotValidOutputRevision()
      throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    Revision revision = new Revision(EMPTY_STRING, REVISION_PROVIDER);
    task.setOutputRevision(revision);

    prepareMocks(NORMALIZATION_TOPOLOGY);
    ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }

  @Test
  public void shouldProperlySendTaskWithFileEntryToNormalizationTopology() throws Exception {

    DpsTask task = getDpsTaskWithFileDataEntry();

    task.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionNAme");
    task.addParameter(PluginParameterKeys.REVISION_PROVIDER, "sampleRevisionProvider");
    task.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-07-12T16:50:00.000Z");
    setCorrectlyFormulatedOutputRevision(task);

    prepareMocks(NORMALIZATION_TOPOLOGY);
    ResultActions response = sendTask(task, NORMALIZATION_TOPOLOGY);

    assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
  }


  @Test
  public void shouldProperlySendTaskWithFileEntryToValidationTopology() throws Exception {
    //given
    DpsTask task = getDpsTaskWithFileDataEntry();
    task.addParameter(SCHEMA_NAME, "edm-internal");
    task.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionNAme");
    task.addParameter(PluginParameterKeys.REVISION_PROVIDER, "sampleRevisionProvider");
    task.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-07-12T16:50:00.000Z");
    setCorrectlyFormulatedOutputRevision(task);

    prepareMocks(VALIDATION_TOPOLOGY);
    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

    assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingRequiredParameter()
      throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    setCorrectlyFormulatedOutputRevision(task);

    prepareMocks(VALIDATION_TOPOLOGY);
    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingOutputRevision() throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

    prepareMocks(VALIDATION_TOPOLOGY);
    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision1()
      throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    Revision revision = new Revision(" ", REVISION_PROVIDER);
    task.setOutputRevision(revision);
    prepareMocks(VALIDATION_TOPOLOGY);

    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision2()
      throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    Revision revision = new Revision(null, REVISION_PROVIDER);
    task.setOutputRevision(revision);
    prepareMocks(VALIDATION_TOPOLOGY);

    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision3()
      throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    Revision revision = new Revision(REVISION_NAME, null);
    task.setOutputRevision(revision);

    prepareMocks(VALIDATION_TOPOLOGY);

    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldProperlySendTaskWithOaiPmhRepository() throws Exception {
    DpsTask task = getDpsTaskWithRepositoryURL(OAI_PMH_REPOSITORY_END_POINT);
    task.addParameter(PROVIDER_ID, PROVIDER_ID);
    task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
    task.addParameter(OUTPUT_DATA_SETS, DATASET_URL);
    OAIPMHHarvestingDetails harvestingDetails = new OAIPMHHarvestingDetails();
    harvestingDetails.setSchema("oai_dc");
    task.setHarvestingDetails(harvestingDetails);
    when(harvestsExecutor.execute(any(OaiHarvest.class), any(SubmitTaskParameters.class))).thenReturn(
        new HarvestResult(1, TaskState.PROCESSED));
    prepareMocks(OAI_TOPOLOGY);

    ResultActions response = sendTask(task, OAI_TOPOLOGY);

    assertNotNull(response);
    response.andExpect(status().isCreated());
    verify(harvestsExecutor).execute(any(OaiHarvest.class), any(SubmitTaskParameters.class));
    verifyNoInteractions(recordKafkaSubmitService);
  }


  @Test
  public void shouldThrowExceptionWhenMissingRequiredProviderId() throws Exception {

    DpsTask task = getDpsTaskWithRepositoryURL(OAI_PMH_REPOSITORY_END_POINT);

    prepareMocks(OAI_TOPOLOGY);
    ResultActions response = sendTask(task, OAI_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldProperlySendTaskWithHTTPRepository() throws Exception {

    DpsTask task = getDpsTaskWithRepositoryURL(HTTP_COMPRESSED_FILE_URL);
    task.addParameter(PROVIDER_ID, PROVIDER_ID);
    task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
    task.addParameter(OUTPUT_DATA_SETS, DATASET_URL);
    task.addParameter(REVISION_NAME, "OAIPMH_HARVEST");
    task.addParameter(REVISION_PROVIDER, "metis_test5");
    task.addParameter(REVISION_TIMESTAMP, "2018-01-31T11:33:30.842+01:00");

    prepareMocks(HTTP_TOPOLOGY);
    ResultActions response = sendTask(task, HTTP_TOPOLOGY);

    assertSuccessfulHttpTopologyRequest(response);
  }


  @Test
  public void shouldThrowExceptionWhenMissingRequiredProviderIdForHttpService() throws Exception {

    DpsTask task = getDpsTaskWithRepositoryURL(HTTP_COMPRESSED_FILE_URL);

    prepareMocks(HTTP_TOPOLOGY);
    ResultActions response = sendTask(task, HTTP_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowExceptionWhenSubmittingTaskToHttpServiceWithNotValidOutputRevision() throws Exception {

    DpsTask task = getDpsTaskWithRepositoryURL(HTTP_COMPRESSED_FILE_URL);

    Revision revision = new Revision(REVISION_NAME, null);
    task.setOutputRevision(revision);
    prepareMocks(HTTP_TOPOLOGY);

    ResultActions response = sendTask(task, HTTP_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }

  @Test
  public void shouldProperlySendTaskWithPreviewAsTargetIndexingDatabase() throws Exception {
    //given
    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
    task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
    task.addParameter(OUTPUT_DATA_SETS, DATASET_URL);
    task.addParameter(REVISION_NAME, "OAIPMH_HARVEST");
    task.addParameter(REVISION_PROVIDER, "metis_test5");
    task.addParameter(REVISION_TIMESTAMP, "2018-01-31T11:33:30.842+01:00");
    task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));

    prepareMocks(INDEXING_TOPOLOGY);
    //when
    ResultActions response = sendTask(task, INDEXING_TOPOLOGY);

    //then
    assertSuccessfulRequest(response, INDEXING_TOPOLOGY);
  }

  @Test
  public void shouldProperlySendTaskWithPublishAsTargetIndexingDatabase() throws Exception {
    //given
    DpsTask task = getDpsTaskWithDataSetEntry();

    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
    task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
    task.addParameter(OUTPUT_DATA_SETS, DATASET_URL);
    task.addParameter(REVISION_NAME, "OAIPMH_HARVEST");
    task.addParameter(REVISION_PROVIDER, "metis_test5");
    task.addParameter(REVISION_TIMESTAMP, "2018-01-31T11:33:30.842+01:00");
    task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));
    prepareMocks(INDEXING_TOPOLOGY);

    //when
    ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

    //then
    sendTaskResponse.andExpect(status().isCreated());
  }


  @Test
  public void shouldProperlySendTaskWithTargetIndexingDatabaseAndFileUrls() throws Exception {
    //given
    DpsTask task = getDpsTaskWithFileDataEntry();
    task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
    task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));
    task.addParameter(HARVEST_DATE, "2021-07-12T16:50:00.000Z");
    task.addParameter(OUTPUT_DATA_SETS, DATASET_URL);
    task.addParameter(REVISION_NAME, "OAIPMH_HARVEST");
    task.addParameter(REVISION_PROVIDER, "metis_test5");
    task.addParameter(REVISION_TIMESTAMP, "2018-01-31T11:33:30.842+01:00");
    prepareMocks(INDEXING_TOPOLOGY);

    //when
    ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

    //then
    sendTaskResponse.andExpect(status().isCreated());
  }


  @Test
  public void shouldThrowExceptionWhenTargetIndexingDatabaseIsMissing() throws Exception {
    //given
    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));
    prepareMocks(INDEXING_TOPOLOGY);

    //when
    ResultActions sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

    //then
    sendTaskResponse.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowExceptionWhenTargetIndexingDatabaseIsNotProper() throws Exception {
    //given
    DpsTask task = new DpsTask("indexingTask");
    task.addDataEntry(DATASET_URLS,
        Collections.singletonList(DATASET_URL));
    task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
    task.addParameter(MIME_TYPE, "image/tiff");
    task.addParameter(REPRESENTATION_NAME, "REPRESENTATION_NAME");
    task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "wrong-value");
    task.setOutputRevision(new Revision("REVISION_NAME", "REVISION_PROVIDER"));
    String topologyName = "indexing_topology";
    prepareMocks(topologyName);

    ResultActions response = sendTask(task, topologyName);

    //then
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldNotSubmitEmptyTask() throws Exception {

    DpsTask task = getDpsTaskWithFileDataEntry();
    task.addParameter(SCHEMA_NAME, "edm-internal");

    task.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionNAme");
    task.addParameter(PluginParameterKeys.REVISION_PROVIDER, "sampleRevisionProvider");
    task.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-07-12T16:50:00.000Z");
    setCorrectlyFormulatedOutputRevision(task);
    prepareMocks(VALIDATION_TOPOLOGY);
    when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(0);

    ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

    response.andExpect(status().isCreated());
    verifyNoInteractions(recordKafkaSubmitService);
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionWhenMissingRepresentationName() throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
    task.addParameter(MIME_TYPE, IMAGE_TIFF);

    prepareMocks(IC_TOPOLOGY);
    ResultActions response = sendTask(task, IC_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowDpsTaskValidationExceptionOnSendTask() throws Exception {

    DpsTask task = getDpsTaskWithFileDataEntry();

    prepareMocks(IC_TOPOLOGY);

    ResultActions response = sendTask(task, IC_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision1() throws Exception {

    DpsTask task = getDpsTaskWithDataSetEntry();
    prepareCompleteParametersForIcTask(task);
    task.setOutputRevision(new Revision(EMPTY_STRING, REVISION_PROVIDER));

    prepareMocks(IC_TOPOLOGY);
    ResultActions response = sendTask(task, IC_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision2() throws Exception {
    //given
    DpsTask task = getDpsTaskWithDataSetEntry();
    prepareCompleteParametersForIcTask(task);
    task.setOutputRevision(new Revision(EMPTY_STRING, EMPTY_STRING));

    prepareMocks(IC_TOPOLOGY);
    ResultActions response = sendTask(task, IC_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision3() throws Exception {
    //given
    DpsTask task = getDpsTaskWithDataSetEntry();
    prepareCompleteParametersForIcTask(task);
    task.setOutputRevision(new Revision(null, null));

    prepareMocks(IC_TOPOLOGY);
    ResultActions response = sendTask(task, IC_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldGetProgressReport() throws Exception {

    TaskInfo taskInfo = TaskInfo.builder()
                                .id(TASK_ID)
                                .topologyName(TOPOLOGY_NAME)
                                .state(TaskState.PROCESSED)
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
  public void shouldKillTheTask() throws Exception {
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
  public void shouldKillTheTaskWhenPassingTheCauseOfKilling() throws Exception {
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
  public void killTaskShouldFailForNonExistedTopology() throws Exception {
    doNothing().when(reportService).checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(false);

    ResultActions response = mockMvc.perform(
        post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
    );
    response.andExpect(status().isMethodNotAllowed());
  }


  @Test
  public void killTaskShouldFailWhenTaskDoesNotBelongToTopology() throws Exception {
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
    doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService)
                                                            .checkIfTaskExists(TASK_ID, TOPOLOGY_NAME);

    ResultActions response = mockMvc.perform(
        post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
    );
    response.andExpect(status().isMethodNotAllowed());
  }


  @Test
  public void shouldThrowExceptionIfTaskIdWasNotFound() throws Exception {
    when(reportService.getTaskProgress(TASK_ID)).thenThrow(AccessDeniedOrObjectDoesNotExistException.class);
    when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);

    ResultActions response = mockMvc.perform(
        get(PROGRESS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
    );
    response.andExpect(status().isMethodNotAllowed());
  }


  @Test
  public void shouldProperlySendTaskWithDataSetEntryAndRevisionToLinkCheckTopology() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    prepareTaskWithRepresentationAndRevision(task);
    prepareMocks(LINK_CHECKING_TOPOLOGY);

    ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);
    assertSuccessfulRequest(response, LINK_CHECKING_TOPOLOGY);
  }


  @Test
  public void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToLinkCheckTopology() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

    prepareMocks(LINK_CHECKING_TOPOLOGY);
    ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);

    assertSuccessfulRequest(response, LINK_CHECKING_TOPOLOGY);
  }


  @Test
  public void shouldThrowValidationExceptionWhenSendingTaskToLinkCheckWithWrongDataSetURL() throws Exception {
    DpsTask task = new DpsTask(TASK_NAME);
    task.addDataEntry(DATASET_URLS, Collections.singletonList(WRONG_DATA_SET_URL));
    prepareMocks(LINK_CHECKING_TOPOLOGY);

    ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);

    response.andExpect(status().isBadRequest());
  }


  @Test
  public void shouldThrowValidationExceptionWhenSendingTaskToLinkCheckTopologyWithNotValidOutputRevision() throws Exception {
    DpsTask task = getDpsTaskWithDataSetEntry();
    Revision revision = new Revision(" ", REVISION_PROVIDER);
    task.setOutputRevision(revision);
    prepareMocks(LINK_CHECKING_TOPOLOGY);

    ResultActions response = sendTask(task, LINK_CHECKING_TOPOLOGY);
    response.andExpect(status().isBadRequest());
  }

  /* Depublication */
  @Test
  public void shouldSupportDepublication() throws Exception {
    prepareMocks(DEPUBLICATION_TOPOLOGY);
    DpsTask task = new DpsTask(TASK_NAME);
    task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
    task.addParameter(DEPUBLICATION_REASON, "reason");

    sendTask(task, DEPUBLICATION_TOPOLOGY)
        .andExpect(status().isCreated());
  }

  @Test
  public void shouldDepublicationThrowsValidationExceptionWhenTryingWithDatasetUrls() throws Exception {
    prepareMocks(DEPUBLICATION_TOPOLOGY);
    DpsTask task = getDpsTaskWithDataSetEntry();

    sendTask(task, DEPUBLICATION_TOPOLOGY)
        .andExpect(status().isBadRequest());
  }

  @Test
  public void shouldDepublicationThrowsValidationExceptionWhenTryingWithFileUrls() throws Exception {
    prepareMocks(DEPUBLICATION_TOPOLOGY);
    DpsTask task = getDpsTaskWithFileDataEntry();

    sendTask(task, DEPUBLICATION_TOPOLOGY)
        .andExpect(status().isBadRequest());
  }

  @Test
  public void shouldDepublicationThrowsValidationExceptionWhenTryingWithRepositoryUrls() throws Exception {
    prepareMocks(DEPUBLICATION_TOPOLOGY);
    DpsTask task = getDpsTaskWithRepositoryURL("http://xxx.yy");

    sendTask(task, DEPUBLICATION_TOPOLOGY)
        .andExpect(status().isBadRequest());
  }

  @Test
  public void shouldDepublicationThrowsValidationExceptionWhenMissingMetisDatasetParameter() throws Exception {
    prepareMocks(DEPUBLICATION_TOPOLOGY);
    DpsTask task = new DpsTask(TASK_NAME);

    sendTask(task, DEPUBLICATION_TOPOLOGY)
        .andExpect(status().isBadRequest());
  }

  @Test
  public void shouldPassValidParametersToDepublicationService() throws Exception {
    prepareMocks(DEPUBLICATION_TOPOLOGY);
    DpsTask task = new DpsTask(TASK_NAME);
    task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
    task.addParameter(RECORD_IDS_TO_DEPUBLISH, SAMPLE_RECORD_LIST);
    task.addParameter(DEPUBLICATION_REASON, "reason");

    sendTask(task, DEPUBLICATION_TOPOLOGY)
        .andExpect(status().isCreated());

    ArgumentCaptor<SubmitTaskParameters> captor = ArgumentCaptor.forClass(SubmitTaskParameters.class);
    verify(depublicationTaskSubmitter).submitTask(captor.capture());
    assertEquals(SAMPLE_DATASET_METIS_ID, captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID));
    assertEquals(SAMPLE_RECORD_LIST, captor.getValue().getTask().getParameter(RECORD_IDS_TO_DEPUBLISH));
  }

  @Test
  public void shouldPassParametersWhenNoRecordsSelected() throws Exception {
    prepareMocks(DEPUBLICATION_TOPOLOGY);
    DpsTask task = new DpsTask(TASK_NAME);
    task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);
    task.addParameter(DEPUBLICATION_REASON, "reason");

    sendTask(task, DEPUBLICATION_TOPOLOGY)
        .andExpect(status().isCreated());

    ArgumentCaptor<SubmitTaskParameters> captor = ArgumentCaptor.forClass(SubmitTaskParameters.class);
    verify(depublicationTaskSubmitter).submitTask(captor.capture());
    assertEquals(SAMPLE_DATASET_METIS_ID, captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID));
    assertNull(captor.getValue().getTask().getParameter(RECORD_IDS_TO_DEPUBLISH));
  }
  /* Utilities */

  private void prepareTaskWithRepresentationAndRevision(DpsTask task) {
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
    setCorrectlyFormulatedOutputRevision(task);
  }

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
    DpsTask task = new DpsTask(TASK_NAME);
    task.addDataEntry(DATASET_URLS, Collections.singletonList(DATASET_URL));
    task.addParameter(METIS_DATASET_ID, SAMPLE_DATASET_METIS_ID);

    task.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionNAme");
    task.addParameter(PluginParameterKeys.REVISION_PROVIDER, "sampleRevisionProvider");
    task.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-07-12T16:50:00.000Z");

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

  public static String asJsonString(final Object obj) {
    try {
      return new ObjectMapper().writeValueAsString(obj);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private DpsTask getDpsTaskWithFileDataEntry() {
    DpsTask task = new DpsTask(TASK_NAME);
    task.addDataEntry(FILE_URLS, Collections.singletonList(TEST_RESOURCE_URL));
    task.addParameter(METIS_DATASET_ID, "sampleDS");
    return task;
  }

  private DpsTask getDpsTaskWithRepositoryURL(String repositoryURL) {
    DpsTask task = new DpsTask(TASK_NAME);
    task.addDataEntry(REPOSITORY_URLS, Collections.singletonList(repositoryURL));
    return task;
  }

  private void prepareCompleteParametersForIcTask(DpsTask task) {
    task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
    task.addParameter(MIME_TYPE, IMAGE_TIFF);
    task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
  }

  private void setCorrectlyFormulatedOutputRevision(DpsTask task) {
    Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
    task.setOutputRevision(revision);
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