package eu.europeana.cloud.service.dps.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.config.DPSServiceTestContext;
import eu.europeana.cloud.service.dps.depublish.DatasetDepublisher;
import eu.europeana.cloud.service.dps.depublish.DepublicationService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.kafka.TaskKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.services.DatasetCleanerService;
import eu.europeana.cloud.service.dps.services.submitters.HttpTopologyTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.OaiTopologyTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.OtherTopologiesTaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.services.validation.TaskSubmissionValidator;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSTaskSubmiter;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusSynchronizer;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.security.acls.model.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.*;

import static eu.europeana.cloud.service.dps.InputDataType.*;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {DPSServiceTestContext.class, TopologyTasksResource.class, TaskSubmitterFactory.class,
        TaskSubmissionValidator.class, SubmitTaskService.class, OaiTopologyTaskSubmitter.class,
        HttpTopologyTaskSubmitter.class, OtherTopologiesTaskSubmitter.class, DatasetCleanerService.class,
        TaskStatusUpdater.class, TaskStatusSynchronizer.class, MCSTaskSubmiter.class})
public class TopologyTasksResourceTest extends AbstractResourceTest {

    /* Endpoints */
    private final static String WEB_TARGET = TopologyTasksResource.class.getAnnotation(RequestMapping.class).value()[0];
    private final static String PROGRESS_REPORT_WEB_TARGET = WEB_TARGET + "/{taskId}/progress";
    private final static String KILL_TASK_WEB_TARGET = WEB_TARGET + "/{taskId}/kill";
    private final static String CLEAN_DATASET_WEB_TARGET = WEB_TARGET + "/{taskId}/cleaner";

    /* Constants */
    private final static String DATA_SET_URL = "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets";
    private final static String IMAGE_TIFF = "image/tiff";
    private final static String IMAGE_JP2 = "image/jp2";
    private final static String IC_TOPOLOGY = "ic_topology";
    private final static  String TASK_NAME = "TASK_NAME";

    private final static String OAI_PMH_REPOSITORY_END_POINT = "http://example.com/oai-pmh-repository.xml";
    private final static String HTTP_COMPRESSED_FILE_URL = "http://example.com/zipFile.zip";
    private final static String WRONG_DATA_SET_URL = "http://wrongDataSet.com";

    private final static String LINK_CHECKING_TOPOLOGY = "linkcheck_topology";
    public static final String SAMPLE_DATASE_METIS_ID = "sampleDS";

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
    private TaskKafkaSubmitService taskKafkaSubmitService;
    
    @Autowired
    private DepublicationService depublicationService;

    @Autowired
    private DatasetDepublisher datasetDepublisher;

    public TopologyTasksResourceTest() {
        super();
    }

    @Before
    public void init() {
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
        taskKafkaSubmitService = applicationContext.getBean(TaskKafkaSubmitService.class);

        reset(
                taskDAO,
                dataSetServiceClient,
                recordKafkaSubmitService,
                reportService,
                taskKafkaSubmitService,
                depublicationService
        );
        when(taskDAO.findById(anyLong())).thenReturn(Optional.empty());
    }


    @Test
    public void shouldProperlySendTask() throws Exception {
        //given
        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(MIME_TYPE, IMAGE_TIFF);
        task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
        prepareMocks(IC_TOPOLOGY);
        ResultActions response = sendTask(task, IC_TOPOLOGY);
        assertSuccessfulRequest(response, IC_TOPOLOGY);
    }


    @Test
    public void shouldProperlySendTaskWithDataSetEntry() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareCompleteParametersForIcTask(task);
        prepareMocks(IC_TOPOLOGY);

        ResultActions response = sendTask(task, IC_TOPOLOGY);

        assertSuccessfulRequest(response, IC_TOPOLOGY);
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
        task.addDataEntry(DATASET_URLS, Arrays.asList(WRONG_DATA_SET_URL));
        prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

        response.andExpect(status().isBadRequest());
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToEnrichmentTopologyWithNotValidOutputRevision() throws Exception {
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
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        doThrow(DataSetNotExistsException.class).when(dataSetServiceClient).getDataSetRepresentationsChunk(anyString(), anyString(), anyString());
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenUnexpectedExceptionHappens() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        doThrow(MCSException.class).when(dataSetServiceClient).getDataSetRepresentationsChunk(anyString(), anyString(), anyString());
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetProviderIsNotEqualToTheProviderIdParameter() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        task.addParameter(PluginParameterKeys.PROVIDER_ID, "DIFFERENT_PROVIDER_ID");
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(new ResultSlice<>());
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
    }


    @Test
    public void shouldProperlySendTaskWhithOutputDataSet() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(PluginParameterKeys.REPRESENTATION_NAME,"exampleParamName");
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(new ResultSlice<>());
        prepareMocks(ENRICHMENT_TOPOLOGY);

        ResultActions response = sendTask(task, ENRICHMENT_TOPOLOGY);

        assertSuccessfulRequest(response, OAI_TOPOLOGY);
    }


    @Test
    public void shouldProperlySendTaskWithFileEntryToEnrichmentTopology() throws Exception {

        DpsTask task = getDpsTaskWithFileDataEntry();
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
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToNormalizationTopologyWithNotValidOutputRevision() throws Exception {

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
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(VALIDATION_TOPOLOGY);
        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

        assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingRequiredParameter() throws Exception {

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
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision1() throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        Revision revision = new Revision(" ", REVISION_PROVIDER);
        task.setOutputRevision(revision);
        prepareMocks(VALIDATION_TOPOLOGY);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision2() throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        Revision revision = new Revision(null, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        prepareMocks(VALIDATION_TOPOLOGY);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);
        response.andExpect(status().isBadRequest());
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision3() throws Exception {

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
        OAIPMHHarvestingDetails harvestingDetails = new OAIPMHHarvestingDetails();
        harvestingDetails.setSchemas(Collections.singleton("oai_dc"));
        task.setHarvestingDetails(harvestingDetails);
        when(harvestsExecutor.execute(anyListOf(Harvest.class), any(SubmitTaskParameters.class))).thenReturn(new HarvestResult(1, TaskState.PROCESSED));
        prepareMocks(OAI_TOPOLOGY);

        ResultActions response = sendTask(task, OAI_TOPOLOGY);

        assertNotNull(response);
        response.andExpect(status().isCreated());
        Thread.sleep( 1000);
        verify(harvestsExecutor).execute(anyListOf(Harvest.class), any(SubmitTaskParameters.class));
        verifyZeroInteractions(taskKafkaSubmitService);
        verifyZeroInteractions(recordKafkaSubmitService);
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

        prepareMocks(HTTP_TOPOLOGY);
        ResultActions response = sendTask(task, HTTP_TOPOLOGY);

        assertSuccessfulHttpTopologyRequest(response, HTTP_TOPOLOGY);
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
    public void shouldProperlySendTaskWithDataSetEntryWithOutputRevision() throws Exception {

        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareCompleteParametersForIcTask(task);
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));

        prepareMocks(IC_TOPOLOGY);
        ResultActions response = sendTask(task, IC_TOPOLOGY);
        assertSuccessfulRequest(response, IC_TOPOLOGY);
    }


    @Test
    public void shouldProperlySendTaskWithPreviewAsTargetIndexingDatabase() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));

        prepareMocks(INDEXING_TOPOLOGY);
        //when
        ResultActions response = sendTask(task, INDEXING_TOPOLOGY);

        //then
        assertSuccessfulRequest(response, INDEXING_TOPOLOGY);
    }

    @Test
    public void shouldProperlySendTaskWithPublishsAsTargetIndexingDatabase() throws Exception {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();

        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
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
        task.addDataEntry(DATASET_URLS, Collections.singletonList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
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
        setCorrectlyFormulatedOutputRevision(task);
        prepareMocks(VALIDATION_TOPOLOGY);
        when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(0);

        ResultActions response = sendTask(task, VALIDATION_TOPOLOGY);

        response.andExpect(status().isCreated());
        Thread.sleep(10000);
        verifyZeroInteractions(taskKafkaSubmitService);
        verifyZeroInteractions(recordKafkaSubmitService);
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

        TaskInfo taskInfo = new TaskInfo(TASK_ID, TOPOLOGY_NAME, TaskState.PROCESSED, EMPTY_STRING, 100, 100, 10, 50, new Date(), new Date(), new Date());

        when(reportService.getTaskProgress(eq(Long.toString(TASK_ID)))).thenReturn(taskInfo);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);

        ResultActions response = mockMvc.perform(get(PROGRESS_REPORT_WEB_TARGET, TOPOLOGY_NAME, TASK_ID));

        TaskInfo resultedTaskInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),TaskInfo.class);
        assertThat(taskInfo, is(resultedTaskInfo));
    }


    @Test
    public void shouldKillTheTask() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
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
        //String info = "Dropped by the user";
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        ResultActions response = mockMvc.perform(
                post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID).queryParam("info", info)
        );
        response.andExpect(status().isOk());
        response.andExpect(content().string("The task was killed because of " + info));
    }


    @Test
    public void killTaskShouldFailForNonExistedTopology() throws Exception {
        String info = "Dropped by the user";
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(false);

        ResultActions response = mockMvc.perform(
                post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
        );
        response.andExpect(status().isMethodNotAllowed());
    }


    @Test
    public void killTaskShouldFailWhenTaskDoesNotBelongToTopology() throws Exception {
        String info = "Dropped by the user";
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));

        ResultActions response = mockMvc.perform(
                post(KILL_TASK_WEB_TARGET, TOPOLOGY_NAME, TASK_ID)
        );
        response.andExpect(status().isMethodNotAllowed());
    }


    @Test
    public void shouldThrowExceptionIfTaskIdWasNotFound() throws Exception {
        when(reportService.getTaskProgress(eq(Long.toString(TASK_ID)))).thenThrow(AccessDeniedOrObjectDoesNotExistException.class);
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


    @Test
    public void shouldProperlyHandleCleaningRequest() throws Exception {
        DataSetCleanerParameters dataSetCleanerParameters = prepareDataSetCleanerParameters();
        mockSecurity(INDEXING_TOPOLOGY);
        ResultActions response = mockMvc.perform(
                post(CLEAN_DATASET_WEB_TARGET,INDEXING_TOPOLOGY,TASK_ID)
                        .content(asJsonString(dataSetCleanerParameters))
                        .contentType(MediaType.APPLICATION_JSON));

        assertNotNull(response);
        response.andExpect(status().isOk());
        Thread.sleep(1000);
        verify(taskDAO, times(1))
                .setTaskCompletelyProcessed(eq(TASK_ID), eq("Completely process"));
        verify(taskDAO).findById(anyLong());
        verifyNoMoreInteractions(taskDAO);
    }


    @Test
    public void shouldThrowAccessDeniedWithNoCredentials() throws Exception {
        DataSetCleanerParameters dataSetCleanerParameters = prepareDataSetCleanerParameters();

        ResultActions response = mockMvc.perform(
                post(CLEAN_DATASET_WEB_TARGET,INDEXING_TOPOLOGY,TASK_ID)
                        .content(asJsonString(dataSetCleanerParameters))
                        .contentType(MediaType.APPLICATION_JSON));
        response.andExpect(status().isMethodNotAllowed());
    }


    @Test
    public void shouldDropTaskWhenCleanerParametersAreNull() throws Exception {
        mockSecurity(INDEXING_TOPOLOGY);
        ResultActions response = mockMvc.perform(
                post(CLEAN_DATASET_WEB_TARGET, INDEXING_TOPOLOGY, TASK_ID)
                        .content(asJsonString(new DataSetCleanerParameters()))
                        .contentType(MediaType.APPLICATION_JSON)

        );
        assertNotNull(response);
        response.andExpect(status().isOk());
        Thread.sleep(1000);
        verify(taskDAO, times(1))
                .setTaskDropped(eq(TASK_ID),
                        eq("cleaner parameters can not be null")
                );
        verify(taskDAO).findById(anyLong());
        verifyNoMoreInteractions(taskDAO);
    }

    /* Depublication */
    @Test
    public void shouldSupportDepublication() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = new DpsTask(TASK_NAME);
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASE_METIS_ID);

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
    public void shouldPassValidParameterToDepublicationService() throws Exception {
        prepareMocks(DEPUBLICATION_TOPOLOGY);
        DpsTask task = new DpsTask(TASK_NAME);
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASE_METIS_ID);
        task.addParameter(METIS_USE_ALT_INDEXING_ENV,"true");

        sendTask(task,DEPUBLICATION_TOPOLOGY)
                .andExpect(status().isCreated());
        Thread.sleep(200L);

        ArgumentCaptor<SubmitTaskParameters> captor= ArgumentCaptor.forClass(SubmitTaskParameters.class);
        verify(depublicationService).depublishDataset(captor.capture());
        assertTrue(Boolean.valueOf(captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV)));
        assertEquals(SAMPLE_DATASE_METIS_ID, captor.getValue().getTask().getParameter(PluginParameterKeys.METIS_DATASET_ID));
    }

    /* Utilities */

    private void prepareTaskWithRepresentationAndRevision(DpsTask task) {
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        setCorrectlyFormulatedOutputRevision(task);
    }

    private void assertSuccessfulHttpTopologyRequest(ResultActions response, String topologyName) throws Exception {
        assertNotNull(response);
        response.andExpect(status().isCreated());
        Thread.sleep(5000);
        verify(taskKafkaSubmitService).submitTask(any(DpsTask.class), eq(topologyName));
        verifyNoMoreInteractions(taskKafkaSubmitService);
        verifyZeroInteractions(recordKafkaSubmitService);
        //verify(recordKafkaSubmitService).submitRecord(any(DpsRecord.class), eq(topologyName));
    }

    private void assertSuccessfulRequest(ResultActions response, String topologyName) throws Exception {
        assertNotNull(response);
        response.andExpect(status().isCreated());
        Thread.sleep(5000);
        verifyZeroInteractions(taskKafkaSubmitService);
    }

    private DataSetCleanerParameters prepareDataSetCleanerParameters() {
        DataSetCleanerParameters dataSetCleanerParameters = new DataSetCleanerParameters();
        dataSetCleanerParameters.setCleaningDate(new Date());
        dataSetCleanerParameters.setDataSetId("DATASET_ID");
        dataSetCleanerParameters.setUsingAltEnv(true);
        dataSetCleanerParameters.setTargetIndexingEnv(TargetIndexingDatabase.PREVIEW.toString());
        return dataSetCleanerParameters;
    }

    private DpsTask getDpsTaskWithDataSetEntry() {
        DpsTask task = new DpsTask(TASK_NAME);
        task.addDataEntry(DATASET_URLS, Collections.singletonList(DATA_SET_URL));
        task.addParameter(METIS_DATASET_ID, SAMPLE_DATASE_METIS_ID);
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

    private List<SubTaskInfo> createDummySubTaskInfoList() {
        List<SubTaskInfo> subTaskInfoList = new ArrayList<>();
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, RESOURCE_URL, RecordState.SUCCESS, EMPTY_STRING, EMPTY_STRING, RESULT_RESOURCE_URL);
        subTaskInfoList.add(subTaskInfo);
        return subTaskInfoList;
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


    private void prepareMocks(String topologyName) throws MCSException, TaskSubmissionException {
        //Mock security
        mockSecurity(topologyName);
        mockECloudClients();
    }

    private void mockSecurity(String topologyName) {
        MutableAcl mutableAcl = mock(MutableAcl.class);
        //Mock
        when(topologyManager.containsTopology(topologyName)).thenReturn(true);
        when(mutableAcl.getEntries()).thenReturn(Collections.EMPTY_LIST);
        doNothing().when(mutableAcl).insertAce(anyInt(), any(Permission.class), any(Sid.class), anyBoolean());
        doNothing().when(taskDAO).insert(anyLong(), anyString(), anyInt(), anyInt(), anyString(), anyString(), isA(Date.class), isA(Date.class), isA(Date.class), anyInt(), anyString());
        when(mutableAclService.readAclById(any(ObjectIdentity.class))).thenReturn(mutableAcl);
        when(context.getBean(RecordServiceClient.class)).thenReturn(recordServiceClient);
    }

    private void mockECloudClients() throws TaskSubmissionException, MCSException {
        when(context.getBean(FileServiceClient.class)).thenReturn(fileServiceClient);
        when(context.getBean(DataSetServiceClient.class)).thenReturn(dataSetServiceClient);
        when(filesCounterFactory.createFilesCounter(any(DpsTask.class),anyString())).thenReturn(filesCounter);
        when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(1);
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(eu.europeana.cloud.common.model.Permission.class));
    }

}