package eu.europeana.cloud.service.dps.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.kafka.TaskKafkaSubmitService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.service.cassandra.CassandraValidationStatisticsService;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.model.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.WebApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

import static eu.europeana.cloud.service.dps.InputDataType.*;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpiedDpsTestContext.class,TopologyTasksResource.class})
@WebAppConfiguration
@TestPropertySource(properties = {"numberOfElementsOnPage=100","maxIdentifiersCount=100"})
public class TopologyTasksResourceTest /*extends JerseyTest*/ {
    private static final String DATA_SET_URL = "http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets";
    private static final String IMAGE_TIFF = "image/tiff";
    private static final String IMAGE_JP2 = "image/jp2";
    private static final String IC_TOPOLOGY = "ic_topology";
    private static final String RESOURCE_URL = "http://tomcat:8080/mcs/records/ZU5NI2ILYC6RMUZRB53YLIWXPNYFHL5VCX7HE2JCX7OLI2OLIGNQ/representations/SOURCE-REPRESENTATION/versions/SOURCE_VERSION/files/SOURCE_FILE";
    private static final String RESULT_RESOURCE_URL = "http://tomcat:8080/mcs/records/ZU5NI2ILYC6RMUZRB53YLIWXPNYFHL5VCX7HE2JCX7OLI2OLIGNQ/representations/DESTINATION-REPRESENTATION/versions/destination_VERSION/files/DESTINATION_FILE";
    private static final long TASK_ID = 12345;
    private static final String TOPOLOGY_NAME = "ANY_TOPOLOGY";
    private static final String TASK_NAME = "TASK_NAME";

    private static final String ERROR_MESSAGE = "Message";
    private static final String[] ERROR_TYPES = {"bd0c7280-db47-11e7-ada4-e2f54b49d956", "bd0ac4d0-db47-11e7-ada4-e2f54b49d956", "4bb74640-db48-11e7-af3d-e2f54b49d956"};
    private static final int[] ERROR_COUNTS = {5, 2, 7};
    private static final String ERROR_RESOURCE_IDENTIFIER = "Resource id ";
    private static final String ADDITIONAL_INFORMATIONS = "Additional informations ";
    private static final String OAI_PMH_REPOSITORY_END_POINT = "http://example.com/oai-pmh-repository.xml";
    private static final String HTTP_COMPRESSED_FILE_URL = "http://example.com/zipFile.zip";
    private static final String TOPOLOGY_NAME_PARAMETER_LABEL = "topologyName";
    private static final String TASK_ID_PARAMETER_LABEL = "taskId";
    private static final String EMPTY_STRING = "";
    private static final String WRONG_DATA_SET_URL = "http://wrongDataSet.com";
    public static final String PATH = "path";
    public static final String PATH_VALUE = "ELEMENT";

    private static final String LINK_CHECKING_TOPOLOGY = "linkcheck_topology";

    private TopologyManager topologyManager;
    private MutableAclService mutableAclService;
    private String webTarget;
    private String detailedReportWebTarget;
    private String progressReportWebTarget;
    private String killTaskWebTarget;
    private String errorsReportWebTarget;
    private String validationStatisticsReportWebTarget;
    private String elementReportWebTarget;
    private String cleanDatasetWebTarget;
    private RecordServiceClient recordServiceClient;
    private ApplicationContext context;
    private DataSetServiceClient dataSetServiceClient;
    private FileServiceClient fileServiceClient;
    private CassandraTaskInfoDAO taskDAO;
    private FilesCounterFactory filesCounterFactory;
    private FilesCounter filesCounter;
    private TaskKafkaSubmitService taskKafkaSubmitService;
    private RecordKafkaSubmitService recordKafkaSubmitService;
    private TaskExecutionReportService reportService;
    private TaskExecutionKillService killService;
    private ValidationStatisticsReportService validationStatisticsService;
    private HarvestsExecutor harvestsExecutor;

    public TopologyTasksResourceTest() {
    }

//    //@Override
//    protected Application configure() {
//        //return new JerseyConfig().property("contextConfigLocation", "classpath:spiedDpsTestContext.xml");
//        return null;
//    }

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext applicationContext;

    @Before
    public void init() {
        mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext)
                //.apply(springSecurity())
                .build();
       // ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        topologyManager = applicationContext.getBean(TopologyManager.class);
        mutableAclService = applicationContext.getBean(MutableAclService.class);
        recordServiceClient = applicationContext.getBean(RecordServiceClient.class);
        filesCounterFactory = applicationContext.getBean(FilesCounterFactory.class);
        filesCounter = applicationContext.getBean(FilesCounter.class);
        context = applicationContext.getBean(ApplicationContext.class);
        reportService = applicationContext.getBean(TaskExecutionReportService.class);
        killService = applicationContext.getBean(TaskExecutionKillService.class);
        validationStatisticsService = applicationContext.getBean(CassandraValidationStatisticsService.class);
        dataSetServiceClient = applicationContext.getBean(DataSetServiceClient.class);
        fileServiceClient = applicationContext.getBean(FileServiceClient.class);
        taskDAO = applicationContext.getBean(CassandraTaskInfoDAO.class);
        taskKafkaSubmitService = applicationContext.getBean(TaskKafkaSubmitService.class);
        recordKafkaSubmitService = applicationContext.getBean(RecordKafkaSubmitService.class);
        harvestsExecutor = applicationContext.getBean(HarvestsExecutor.class);

        webTarget = TopologyTasksResource.class.getAnnotation(RequestMapping.class).value()[0];
        detailedReportWebTarget = webTarget + "/{taskId}/reports/details";
        progressReportWebTarget = webTarget + "/{taskId}/progress";
        cleanDatasetWebTarget = webTarget + "/{taskId}/cleaner";
        errorsReportWebTarget = webTarget + "/{taskId}/reports/errors";
        validationStatisticsReportWebTarget = webTarget + "/{taskId}/statistics";
        elementReportWebTarget = webTarget + "/{taskId}/reports/element";
        killTaskWebTarget = webTarget + "/{taskId}/kill";

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

    private void prepareTaskWithRepresentationAndRevision(DpsTask task) {
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        setCorrectlyFormulatedOutputRevision(task);
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
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(new ResultSlice<Representation>());
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        response.andExpect(status().isBadRequest());
    }

    @Test
    public void shouldProperlySendTaskWhithOutputDataSet() throws Exception {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(new ResultSlice<Representation>());
        prepareMocks(TOPOLOGY_NAME);

        ResultActions response = sendTask(task, TOPOLOGY_NAME);

        assertSuccessfulRequest(response, TOPOLOGY_NAME);
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
        task.addDataEntry(DATASET_URLS, Arrays.asList(WRONG_DATA_SET_URL));

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
        when(harvestsExecutor.execute(anyString(),anyList(),any(DpsTask.class),anyString())).thenReturn(new HarvestResult(1, TaskState.PROCESSED));
        prepareMocks(OAI_TOPOLOGY);

        ResultActions response = sendTask(task, OAI_TOPOLOGY);

        assertNotNull(response);
        response.andExpect(status().isCreated());
        Thread.sleep( 1000);
        verify(harvestsExecutor).execute(eq(OAI_TOPOLOGY),anyList(),any(DpsTask.class),anyString());
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

        assertSuccessfulRequest(response, HTTP_TOPOLOGY);
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
        task.addDataEntry(DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
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
    public void shouldGetStatisticReport() throws Exception {
        when(validationStatisticsService.getTaskStatisticsReport(TASK_ID)).thenReturn(new StatisticsReport(TASK_ID, null));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        ResultActions response = mockMvc.perform(get(validationStatisticsReportWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isOk()).andExpect(jsonPath("taskId").value(TASK_ID));

    }


    @Test
    public void shouldGetElementReport() throws Exception {
        NodeReport nodeReport = new NodeReport("VALUE", 5, Arrays.asList(new AttributeStatistics("Attr1", "Value1", 10)));
        when(validationStatisticsService.getElementReport(TASK_ID, PATH_VALUE)).thenReturn(Arrays.asList(nodeReport));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        ResultActions response = mockMvc.perform(get(elementReportWebTarget, TOPOLOGY_NAME, TASK_ID).queryParam(PATH, PATH_VALUE));

        response.andExpect(status().isOk());
    }


    @Test
    public void shouldReturn405WhenStatisticsRequestedButTopologyNotFound() throws Exception {
        when(validationStatisticsService.getTaskStatisticsReport(TASK_ID)).thenReturn(new StatisticsReport(TASK_ID, null));
        when(topologyManager.containsTopology(anyString())).thenReturn(false);
        ResultActions response = mockMvc.perform(get(validationStatisticsReportWebTarget, TOPOLOGY_NAME, TASK_ID));
       response.andExpect(status().isMethodNotAllowed());
    }

    @Test
    public void shouldGetDetailedReportForTheFirst100Resources() throws Exception {
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(1), eq(100))).thenReturn(subTaskInfoList);

        ResultActions response = mockMvc.perform(get(detailedReportWebTarget, TOPOLOGY_NAME, TASK_ID));

        assertDetailedReportResponse(subTaskInfoList.get(0), response);

    }

    @Test
    public void shouldThrowExceptionWhenTaskDoesNotBelongToTopology() throws Exception {
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(1), eq(100))).thenReturn(subTaskInfoList);
        ResultActions response = mockMvc.perform(get(detailedReportWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isMethodNotAllowed());
    }

    @Test
    public void shouldGetProgressReport() throws Exception {

        TaskInfo taskInfo = new TaskInfo(TASK_ID, TOPOLOGY_NAME, TaskState.PROCESSED, EMPTY_STRING, 100, 100, 50, new Date(), new Date(), new Date());

        when(reportService.getTaskProgress(eq(Long.toString(TASK_ID)))).thenReturn(taskInfo);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);

        ResultActions response = mockMvc.perform(get(progressReportWebTarget, TOPOLOGY_NAME, TASK_ID));

        TaskInfo resultedTaskInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),TaskInfo.class);
        assertThat(taskInfo, is(resultedTaskInfo));
    }

    @Test
    public void shouldKillTheTask() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        String info = "Dropped by the user";
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));

        ResultActions response = mockMvc.perform(post(killTaskWebTarget, TOPOLOGY_NAME, TASK_ID));

        response.andExpect(status().isOk());
        response.andExpect(content().string("The task was killed because of " + info));
    }


    @Test
    public void shouldKillTheTaskWhenPassingTheCauseOfKilling() throws Exception {
        String info = "The aggregator decided to do so";
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));
        ResultActions response = mockMvc.perform(post(killTaskWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isOk());
        response.andExpect(content().string("The task was killed because of " + info));

    }


    @Test
    public void killTaskShouldFailForNonExistedTopology() throws Exception {
        String info = "Dropped by the user";
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(false);
        ResultActions response = mockMvc.perform(post(killTaskWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isMethodNotAllowed());
    }

    @Test
    public void killTaskShouldFailWhenTaskDoesNotBelongToTopology() throws Exception {
        String info = "Dropped by the user";
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        ResultActions response = mockMvc.perform(post(killTaskWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isMethodNotAllowed());
    }

    @Test
    public void shouldGetGeneralErrorReportWithIdentifiers() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
        when(reportService.getGeneralTaskErrorReport(eq(Long.toString(TASK_ID)), eq(10))).thenReturn(errorsInfo);

        ResultActions response = mockMvc.perform(get(errorsReportWebTarget, TOPOLOGY_NAME, TASK_ID).queryParam("idsCount", "10"));
        TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }


    @Test
    public void shouldCheckIfReportExists() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(reportService.checkIfReportExists(eq(Long.toString(TASK_ID)))).thenReturn(true);
        ResultActions response = mockMvc.perform(head(errorsReportWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isOk());
    }

    @Test
    public void shouldReturn405InCaseOfException() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfReportExists(eq(Long.toString(TASK_ID)));
        ResultActions response = mockMvc.perform(head(errorsReportWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isMethodNotAllowed());

    }

    @Test
    public void shouldGetSpecificErrorReport() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
        when(reportService.getSpecificTaskErrorReport(eq(Long.toString(TASK_ID)), eq(ERROR_TYPES[0]), eq(100))).thenReturn(errorsInfo);

        ResultActions response = mockMvc.perform(get(errorsReportWebTarget, TOPOLOGY_NAME, TASK_ID).queryParam("error", ERROR_TYPES[0]));
        TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }

    @Test
    public void shouldGetGeneralErrorReport() throws Exception {
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(false);
        when(reportService.getGeneralTaskErrorReport(eq(Long.toString(TASK_ID)), eq(0))).thenReturn(errorsInfo);

        ResultActions response = mockMvc.perform(get(errorsReportWebTarget, TOPOLOGY_NAME, TASK_ID));

        TaskErrorsInfo retrievedInfo = new ObjectMapper().readValue(response.andReturn().getResponse().getContentAsString(),TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }


    @Test
    public void shouldThrowExceptionIfTaskIdWasNotFound() throws Exception {
        when(reportService.getTaskProgress(eq(Long.toString(TASK_ID)))).thenThrow(AccessDeniedOrObjectDoesNotExistException.class);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        ResultActions response = mockMvc.perform(get(progressReportWebTarget, TOPOLOGY_NAME, TASK_ID));
        response.andExpect(status().isMethodNotAllowed());
    }

    @Test
    public void shouldGetDetailedReportForSpecifiedResources() throws Exception {
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(120), eq(150))).thenReturn(subTaskInfoList);
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        ResultActions response = mockMvc.perform(get(detailedReportWebTarget, TOPOLOGY_NAME, TASK_ID).queryParam("from", "120").queryParam("to", "150"));
        assertDetailedReportResponse(subTaskInfoList.get(0), response);
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
        task.addDataEntry(DATASET_URLS, Arrays.asList(WRONG_DATA_SET_URL));
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
                post(cleanDatasetWebTarget,INDEXING_TOPOLOGY,TASK_ID)
                        .content(asJsonString(dataSetCleanerParameters))
                        .contentType(MediaType.APPLICATION_JSON));

        assertNotNull(response);
        response.andExpect(status().isOk());
        Thread.sleep(1000);
        verify(taskDAO, times(1)).setTaskStatus(eq(TASK_ID), eq("Completely process"), eq(TaskState.PROCESSED.toString()));
        verifyNoMoreInteractions(taskDAO);
    }

    @Test
    public void shouldThrowAccessDeniedWithNoCredentials() throws Exception {
        DataSetCleanerParameters dataSetCleanerParameters = prepareDataSetCleanerParameters();
        ResultActions response = mockMvc.perform(
                post(cleanDatasetWebTarget,INDEXING_TOPOLOGY,TASK_ID)
                        .content(asJsonString(dataSetCleanerParameters))
                        .contentType(MediaType.APPLICATION_JSON));
        assertNotNull(response);
        response.andExpect(status().isMethodNotAllowed());

    }

    @Test
    public void shouldDropTaskWhenCleanerParametersAreNull() throws Exception {
        mockSecurity(INDEXING_TOPOLOGY);
        ResultActions response = mockMvc.perform(
                post(cleanDatasetWebTarget,INDEXING_TOPOLOGY,TASK_ID));
        assertNotNull(response);
        response.andExpect(status().isOk());
        Thread.sleep(1000);
        verify(taskDAO, times(1)).dropTask(eq(TASK_ID), eq("cleaner parameters can not be null"), eq(TaskState.DROPPED.toString()));
        verifyNoMoreInteractions(taskDAO);

    }

    private void assertSuccessfulRequest(ResultActions response, String topologyName) throws Exception {
        assertNotNull(response);
        response.andExpect(status().isCreated());
        Thread.sleep(5000);
        verify(taskKafkaSubmitService).submitTask(any(DpsTask.class), eq(topologyName));
        verifyNoMoreInteractions(taskKafkaSubmitService);

        verifyZeroInteractions(recordKafkaSubmitService);
        //verify(recordKafkaSubmitService).submitRecord(any(DpsRecord.class), eq(topologyName));
    }

    private DataSetCleanerParameters prepareDataSetCleanerParameters() {
        DataSetCleanerParameters dataSetCleanerParameters = new DataSetCleanerParameters();
        dataSetCleanerParameters.setCleaningDate(new Date());
        dataSetCleanerParameters.setDataSetId("DATASET_ID");
        dataSetCleanerParameters.setIsUsingALtEnv(true);
        dataSetCleanerParameters.setTargetIndexingEnv(TargetIndexingDatabase.PREVIEW.toString());
        return dataSetCleanerParameters;
    }


    private TaskErrorsInfo createDummyErrorsInfo(boolean specific) {
        TaskErrorsInfo info = new TaskErrorsInfo(TASK_ID);
        List<TaskErrorInfo> errors = new ArrayList<>();
        info.setErrors(errors);
        for (int i = 0; i < 3; i++) {
            TaskErrorInfo error = new TaskErrorInfo();
            error.setMessage(ERROR_MESSAGE);
            error.setErrorType(ERROR_TYPES[i]);
            error.setOccurrences(ERROR_COUNTS[i]);
            if (specific) {
                List<ErrorDetails> errorDetails = new ArrayList<>();
                error.setErrorDetails(errorDetails);
                for (int j = 0; j < ERROR_COUNTS[i]; j++) {
                    errorDetails.add(new ErrorDetails(ERROR_RESOURCE_IDENTIFIER + j, ADDITIONAL_INFORMATIONS + j));
                }
            }
            errors.add(error);
        }
        return info;
    }

    private DpsTask getDpsTaskWithDataSetEntry() {
        DpsTask task = new DpsTask(TASK_NAME);
        task.addDataEntry(DATASET_URLS, Arrays.asList(DATA_SET_URL));
        task.addParameter(METIS_DATASET_ID, "sampleDS");
        return task;
    }

    private ResultActions sendTask(DpsTask task, String topologyName) throws Exception {
        return mockMvc.perform(
                post(webTarget,topologyName)
                .content(asJsonString(task))
                .contentType(MediaType.APPLICATION_JSON));
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
        task.addDataEntry(FILE_URLS, Arrays.asList(RESOURCE_URL));
        task.addParameter(METIS_DATASET_ID, "sampleDS");
        return task;
    }

    private DpsTask getDpsTaskWithRepositoryURL(String repositoryURL) {
        DpsTask task = new DpsTask(TASK_NAME);
        task.addDataEntry(REPOSITORY_URLS, Arrays.asList
                (repositoryURL));
        return task;
    }

    private List<SubTaskInfo> createDummySubTaskInfoList() {
        List<SubTaskInfo> subTaskInfoList = new ArrayList<>();
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, RESOURCE_URL, RecordState.SUCCESS, EMPTY_STRING, EMPTY_STRING, RESULT_RESOURCE_URL);
        subTaskInfoList.add(subTaskInfo);
        return subTaskInfoList;
    }

    private void assertDetailedReportResponse(SubTaskInfo subTaskInfo, ResultActions detailedReportResponse) throws Exception {
        detailedReportResponse.andExpect(status().isOk());
        String resultString = detailedReportResponse.andReturn().getResponse().getContentAsString();

        List<SubTaskInfo> resultedSubTaskInfoList = new ObjectMapper().readValue(resultString,new TypeReference<List<SubTaskInfo>>() {
        });
        assertThat(resultedSubTaskInfoList.get(0), is(subTaskInfo));
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
        HashMap<String, String> user = new HashMap<>();
        user.put(topologyName, "Smith");
        MutableAcl mutableAcl = mock(MutableAcl.class);
        //Mock
        when(topologyManager.containsTopology(topologyName)).thenReturn(true);
        when(mutableAcl.getEntries()).thenReturn(Collections.EMPTY_LIST);
        doNothing().when(mutableAcl).insertAce(anyInt(), any(Permission.class), any(Sid.class), anyBoolean());
        doNothing().when(taskDAO).insert(anyLong(), anyString(), anyInt(), anyString(), anyString(), isA(Date.class), anyString());
        when(mutableAclService.readAclById(any(ObjectIdentity.class))).thenReturn(mutableAcl);
        when(context.getBean(RecordServiceClient.class)).thenReturn(recordServiceClient);
    }

    private void mockECloudClients() throws TaskSubmissionException, MCSException {
        when(context.getBean(FileServiceClient.class)).thenReturn(fileServiceClient);
        when(context.getBean(DataSetServiceClient.class)).thenReturn(dataSetServiceClient);
        when(filesCounterFactory.createFilesCounter(anyString())).thenReturn(filesCounter);
        when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(1);
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(eu.europeana.cloud.common.model.Permission.class));
    }

}