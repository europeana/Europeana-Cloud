package eu.europeana.cloud.service.dps.rest;

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
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.model.*;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

import static eu.europeana.cloud.service.dps.InputDataType.*;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;


public class TopologyTasksResourceTest extends JerseyTest {
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
    private static final String VALIDATION_TOPOLOGY = "validation_topology";
    private static final String NORMALIZATION_TOPOLOGY = "normalization_topology";
    private static final String OAI_TOPOLOGY = "oai_topology";
    private static final String INDEXING_TOPOLOGY = "indexing_topology";
    private static final String OAI_PMH_REPOSITORY_END_POINT = "http://example.com/oai-pmh-repository.xml";
    private static final String HTTP_COMPRESSED_FILE_URL = "http://example.com/zipFile.zip";
    private static final String HTTP_TOPOLOGY = "http_topology";
    private static final String ENRICHMENT_TOPOLOGY = "enrichment_topology";
    private static final String TOPOLOGY_NAME_PARAMETER_LABEL = "topologyName";
    private static final String TASK_ID_PARAMETER_LABEL = "taskId";
    private static final String EMPTY_STRING = "";
    private static final String WRONG_DATA_SET_URL = "http://wrongDataSet.com";
    public static final String PATH = "path";
    public static final String PATH_VALUE = "ELEMENT";

    private static final String LINK_CHECKING_TOPOLOGY = "linkcheck_topology";

    private TopologyManager topologyManager;
    private MutableAclService mutableAclService;
    private WebTarget webTarget;
    private WebTarget detailedReportWebTarget;
    private WebTarget progressReportWebTarget;
    private WebTarget killTaskWebTarget;
    private WebTarget errorsReportWebTarget;
    private WebTarget validationStatisticsReportWebTarget;
    private WebTarget elementReportWebTarget;
    private WebTarget cleanDatasetWebTarget;
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

    public TopologyTasksResourceTest() {
    }

    @Override
    protected Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedDpsTestContext.xml");
    }

    @Before
    public void init() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
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
        webTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value());
        detailedReportWebTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value() + "/{taskId}/reports/details");
        progressReportWebTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value() + "/{taskId}/progress");
        cleanDatasetWebTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value() + "/{taskId}/cleaner");
        errorsReportWebTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value() + "/{taskId}/reports/errors");
        validationStatisticsReportWebTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value() + "/{taskId}/statistics");
        elementReportWebTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value() + "/{taskId}/reports/element");
        killTaskWebTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value() + "/{taskId}/kill");
    }

    @Test
    public void shouldProperlySendTask() throws MCSException, TaskSubmissionException, InterruptedException {
        //given
        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(MIME_TYPE, IMAGE_TIFF);
        task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
        prepareMocks(IC_TOPOLOGY);
        Response response = sendTask(task, IC_TOPOLOGY);
        assertSuccessfulRequest(response, IC_TOPOLOGY);
    }


    @Test
    public void shouldProperlySendTaskWithDataSetEntry() throws MCSException, TaskSubmissionException, InterruptedException {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareCompleteParametersForIcTask(task);
        prepareMocks(IC_TOPOLOGY);

        Response response = sendTask(task, IC_TOPOLOGY);

        assertSuccessfulRequest(response, IC_TOPOLOGY);
    }


    @Test
    public void shouldProperlySendTaskWithDataSetEntryToValidationTopology() throws MCSException, TaskSubmissionException, InterruptedException {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        task.addParameter(SCHEMA_NAME, "edm-internal");
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(VALIDATION_TOPOLOGY);

        Response response = sendTask(task, VALIDATION_TOPOLOGY);
        assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
    }

    @Test
    public void shouldProperlySendTaskWithDataSetEntryAndRevisionToEnrichmentTopology() throws MCSException, TaskSubmissionException, InterruptedException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareTaskWithRepresentationAndRevision(task);
        prepareMocks(ENRICHMENT_TOPOLOGY);

        Response response = sendTask(task, ENRICHMENT_TOPOLOGY);
        assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
    }

    private void prepareTaskWithRepresentationAndRevision(DpsTask task) {
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        setCorrectlyFormulatedOutputRevision(task);
    }

    @Test
    public void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToEnrichmentTopology() throws MCSException, TaskSubmissionException, InterruptedException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

        prepareMocks(ENRICHMENT_TOPOLOGY);
        Response response = sendTask(task, ENRICHMENT_TOPOLOGY);

        assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
    }

    @Test
    public void shouldThrowDpsWhenSendingTaskToEnrichmentTopologyWithWrongDataSetURL() throws MCSException, TaskSubmissionException {
        DpsTask task = new DpsTask(TASK_NAME);
        task.addDataEntry(DATASET_URLS, Arrays.asList(WRONG_DATA_SET_URL));
        prepareMocks(ENRICHMENT_TOPOLOGY);

        Response response = sendTask(task, ENRICHMENT_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToEnrichmentTopologyWithNotValidOutputRevision() throws MCSException, TaskSubmissionException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(" ", REVISION_PROVIDER);
        task.setOutputRevision(revision);
        prepareMocks(ENRICHMENT_TOPOLOGY);

        Response response = sendTask(task, ENRICHMENT_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetURLIsMalformed() throws MCSException, TaskSubmissionException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "Malformed dataset");
        prepareMocks(TOPOLOGY_NAME);

        Response response = sendTask(task, TOPOLOGY_NAME);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetDoesNotExist() throws MCSException, TaskSubmissionException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        doThrow(DataSetNotExistsException.class).when(dataSetServiceClient).getDataSetRepresentationsChunk(anyString(), anyString(), anyString());
        prepareMocks(TOPOLOGY_NAME);

        Response response = sendTask(task, TOPOLOGY_NAME);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenUnexpectedExceptionHappens() throws MCSException, TaskSubmissionException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        doThrow(MCSException.class).when(dataSetServiceClient).getDataSetRepresentationsChunk(anyString(), anyString(), anyString());
        prepareMocks(TOPOLOGY_NAME);

        Response response = sendTask(task, TOPOLOGY_NAME);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenOutputDataSetProviderIsNotEqualToTheProviderIdParameter() throws MCSException, TaskSubmissionException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        task.addParameter(PluginParameterKeys.PROVIDER_ID, "DIFFERENT_PROVIDER_ID");
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(new ResultSlice<Representation>());
        prepareMocks(TOPOLOGY_NAME);

        Response response = sendTask(task, TOPOLOGY_NAME);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldProperlySendTaskWhithOutputDataSet() throws MCSException, TaskSubmissionException, InterruptedException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(REVISION_NAME, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATA_SET_URL);
        when(dataSetServiceClient.getDataSetRepresentationsChunk(anyString(), anyString(), anyString())).thenReturn(new ResultSlice<Representation>());
        prepareMocks(TOPOLOGY_NAME);

        Response response = sendTask(task, TOPOLOGY_NAME);

        assertSuccessfulRequest(response, TOPOLOGY_NAME);
    }

    @Test
    public void shouldProperlySendTaskWithFileEntryToEnrichmentTopology() throws MCSException, TaskSubmissionException, InterruptedException {

        DpsTask task = getDpsTaskWithFileDataEntry();
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(ENRICHMENT_TOPOLOGY);
        Response response = sendTask(task, ENRICHMENT_TOPOLOGY);

        assertSuccessfulRequest(response, ENRICHMENT_TOPOLOGY);
    }


    @Test
    public void shouldProperlySendTaskWithDataSetEntryAndRevisionToNormalizationTopology() throws MCSException, TaskSubmissionException, InterruptedException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(NORMALIZATION_TOPOLOGY);
        Response response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
    }

    @Test
    public void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToNormalizationTopology() throws MCSException, TaskSubmissionException, InterruptedException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

        prepareMocks(NORMALIZATION_TOPOLOGY);
        Response response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
    }

    @Test
    public void shouldThrowDpsWhenSendingTaskToNormalizationTopologyWithWrongDataSetURL() throws MCSException, TaskSubmissionException {

        DpsTask task = new DpsTask(TASK_NAME);
        task.addDataEntry(DATASET_URLS, Arrays.asList(WRONG_DATA_SET_URL));

        prepareMocks(NORMALIZATION_TOPOLOGY);
        Response response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToNormalizationTopologyWithNotValidOutputRevision() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(EMPTY_STRING, REVISION_PROVIDER);
        task.setOutputRevision(revision);

        prepareMocks(NORMALIZATION_TOPOLOGY);
        Response response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldProperlySendTaskWithFileEntryToNormalizationTopology() throws MCSException, TaskSubmissionException, InterruptedException {

        DpsTask task = getDpsTaskWithFileDataEntry();
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(NORMALIZATION_TOPOLOGY);
        Response response = sendTask(task, NORMALIZATION_TOPOLOGY);

        assertSuccessfulRequest(response, NORMALIZATION_TOPOLOGY);
    }


    @Test
    public void shouldProperlySendTaskWithFileEntryToValidationTopology() throws MCSException, TaskSubmissionException, InterruptedException {
        //given
        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(SCHEMA_NAME, "edm-internal");
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(VALIDATION_TOPOLOGY);
        Response response = sendTask(task, VALIDATION_TOPOLOGY);

        assertSuccessfulRequest(response, VALIDATION_TOPOLOGY);
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingRequiredParameter() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        setCorrectlyFormulatedOutputRevision(task);

        prepareMocks(VALIDATION_TOPOLOGY);
        Response response = sendTask(task, VALIDATION_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyMissingOutputRevision() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

        prepareMocks(VALIDATION_TOPOLOGY);
        Response response = sendTask(task, VALIDATION_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision1() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        Revision revision = new Revision(" ", REVISION_PROVIDER);
        task.setOutputRevision(revision);
        prepareMocks(VALIDATION_TOPOLOGY);

        Response response = sendTask(task, VALIDATION_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision2() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        Revision revision = new Revision(null, REVISION_PROVIDER);
        task.setOutputRevision(revision);
        prepareMocks(VALIDATION_TOPOLOGY);

        Response response = sendTask(task, VALIDATION_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenSendingTaskToValidationTopologyWithNotValidOutputRevision3() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        Revision revision = new Revision(REVISION_NAME, null);
        task.setOutputRevision(revision);

        prepareMocks(VALIDATION_TOPOLOGY);

        Response response = sendTask(task, VALIDATION_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldProperlySendTaskWithOaiPmhRepository() throws MCSException, TaskSubmissionException, InterruptedException {

        DpsTask task = getDpsTaskWithRepositoryURL(OAI_PMH_REPOSITORY_END_POINT);
        task.addParameter(PROVIDER_ID, PROVIDER_ID);

        prepareMocks(OAI_TOPOLOGY);
        Response response = sendTask(task, OAI_TOPOLOGY);

        assertSuccessfulRequest(response, OAI_TOPOLOGY);
    }


    @Test
    public void shouldThrowExceptionWhenMissingRequiredProviderId() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithRepositoryURL(OAI_PMH_REPOSITORY_END_POINT);

        prepareMocks(OAI_TOPOLOGY);
        Response response = sendTask(task, OAI_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldProperlySendTaskWithHTTPRepository() throws MCSException, TaskSubmissionException, InterruptedException {

        DpsTask task = getDpsTaskWithRepositoryURL(HTTP_COMPRESSED_FILE_URL);
        task.addParameter(PROVIDER_ID, PROVIDER_ID);

        prepareMocks(HTTP_TOPOLOGY);
        Response response = sendTask(task, HTTP_TOPOLOGY);

        assertSuccessfulRequest(response, HTTP_TOPOLOGY);
    }


    @Test
    public void shouldThrowExceptionWhenMissingRequiredProviderIdForHttpService() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithRepositoryURL(HTTP_COMPRESSED_FILE_URL);

        prepareMocks(HTTP_TOPOLOGY);
        Response response = sendTask(task, HTTP_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowExceptionWhenSubmittingTaskToHttpServiceWithNotValidOutputRevision() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithRepositoryURL(HTTP_COMPRESSED_FILE_URL);

        Revision revision = new Revision(REVISION_NAME, null);
        task.setOutputRevision(revision);
        prepareMocks(HTTP_TOPOLOGY);

        Response response = sendTask(task, HTTP_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldProperlySendTaskWithDataSetEntryWithOutputRevision() throws MCSException, TaskSubmissionException, InterruptedException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareCompleteParametersForIcTask(task);
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));

        prepareMocks(IC_TOPOLOGY);
        Response response = sendTask(task, IC_TOPOLOGY);
        assertSuccessfulRequest(response, IC_TOPOLOGY);
    }

    @Test
    public void shouldProperlySendTaskWithPreviewAsTargetIndexingDatabase() throws MCSException, TaskSubmissionException, InterruptedException {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));

        prepareMocks(INDEXING_TOPOLOGY);
        //when
        Response response = sendTask(task, INDEXING_TOPOLOGY);

        //then
        assertSuccessfulRequest(response, INDEXING_TOPOLOGY);
    }

    @Test
    public void shouldProperlySendTaskWithPublishsAsTargetIndexingDatabase() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();

        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PUBLISH");
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));
        prepareMocks(INDEXING_TOPOLOGY);

        //when
        Response sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
    }

    @Test
    public void shouldProperlySendTaskWithTargetIndexingDatabaseAndFileUrls() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, "PREVIEW");
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));
        prepareMocks(INDEXING_TOPOLOGY);

        //when
        Response sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
    }


    @Test
    public void shouldThrowExceptionWhenTargetIndexingDatabaseIsMissing() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));
        prepareMocks(INDEXING_TOPOLOGY);

        //when
        Response sendTaskResponse = sendTask(task, INDEXING_TOPOLOGY);

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowExceptionWhenTargetIndexingDatabaseIsNotProper() throws MCSException, TaskSubmissionException {
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
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldNotSubmitEmptyTask() throws MCSException, TaskSubmissionException, InterruptedException {

        DpsTask task = getDpsTaskWithFileDataEntry();
        task.addParameter(SCHEMA_NAME, "edm-internal");
        setCorrectlyFormulatedOutputRevision(task);
        prepareMocks(VALIDATION_TOPOLOGY);
        when(filesCounter.getFilesCount(isA(DpsTask.class))).thenReturn(0);

        Response response = sendTask(task, VALIDATION_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.CREATED.getStatusCode()));
        Thread.sleep(10000);
        verifyZeroInteractions(taskKafkaSubmitService);
        verifyZeroInteractions(recordKafkaSubmitService);
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenMissingRepresentationName() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(OUTPUT_MIME_TYPE, IMAGE_JP2);
        task.addParameter(MIME_TYPE, IMAGE_TIFF);

        prepareMocks(IC_TOPOLOGY);
        Response response = sendTask(task, IC_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSendTask() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithFileDataEntry();

        prepareMocks(IC_TOPOLOGY);

        Response response = sendTask(task, IC_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision1() throws MCSException, TaskSubmissionException {

        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareCompleteParametersForIcTask(task);
        task.setOutputRevision(new Revision(EMPTY_STRING, REVISION_PROVIDER));

        prepareMocks(IC_TOPOLOGY);
        Response response = sendTask(task, IC_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision2() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareCompleteParametersForIcTask(task);
        task.setOutputRevision(new Revision(EMPTY_STRING, EMPTY_STRING));

        prepareMocks(IC_TOPOLOGY);
        Response response = sendTask(task, IC_TOPOLOGY);
        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision3() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareCompleteParametersForIcTask(task);
        task.setOutputRevision(new Revision(null, null));

        prepareMocks(IC_TOPOLOGY);
        Response response = sendTask(task, IC_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldGetStatisticReport() throws TaskSubmissionException, MCSException {
        when(validationStatisticsService.getTaskStatisticsReport(TASK_ID)).thenReturn(new StatisticsReport(TASK_ID, null));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        WebTarget enrichedWebTarget = validationStatisticsReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        Response response = enrichedWebTarget.request().get();
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        assertEquals(TASK_ID, response.readEntity(StatisticsReport.class).getTaskId());
    }


    @Test
    public void shouldGetElementReport() throws TaskSubmissionException, MCSException {
        NodeReport nodeReport = new NodeReport("VALUE", 5, Arrays.asList(new AttributeStatistics("Attr1", "Value1", 10)));
        when(validationStatisticsService.getElementReport(TASK_ID, PATH_VALUE)).thenReturn(Arrays.asList(nodeReport));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        WebTarget enrichedWebTarget = elementReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        Response response = enrichedWebTarget.queryParam(PATH, PATH_VALUE).request().get();
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
    }


    @Test
    public void shouldReturn405WhenStatisticsRequestedButTopologyNotFound() throws AccessDeniedOrObjectDoesNotExistException {
        when(validationStatisticsService.getTaskStatisticsReport(TASK_ID)).thenReturn(new StatisticsReport(TASK_ID, null));
        when(topologyManager.containsTopology(anyString())).thenReturn(false);
        WebTarget enrichedWebTarget = validationStatisticsReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        Response response = enrichedWebTarget.request().get();
        assertEquals(405, response.getStatus());
    }

    @Test
    public void shouldGetDetailedReportForTheFirst100Resources() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = detailedReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(1), eq(100))).thenReturn(subTaskInfoList);
        Response detailedReportResponse = enrichedWebTarget.request().get();
        assertDetailedReportResponse(subTaskInfoList.get(0), detailedReportResponse);

    }

    @Test
    public void shouldThrowExceptionWhenTaskDoesNotBelongToTopology() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = detailedReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(1), eq(100))).thenReturn(subTaskInfoList);
        Response detailedReportResponse = enrichedWebTarget.request().get();
        assertThat(detailedReportResponse.getStatus(), is(Response.Status.METHOD_NOT_ALLOWED.getStatusCode()));
    }

    @Test
    public void shouldGetProgressReport() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = progressReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);

        TaskInfo taskInfo = new TaskInfo(TASK_ID, TOPOLOGY_NAME, TaskState.PROCESSED, EMPTY_STRING, 100, 100, 50, new Date(), new Date(), new Date());

        when(reportService.getTaskProgress(eq(Long.toString(TASK_ID)))).thenReturn(taskInfo);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        Response detailedReportResponse = enrichedWebTarget.request().get();
        TaskInfo resultedTaskInfo = detailedReportResponse.readEntity(TaskInfo.class);
        assertThat(taskInfo, is(resultedTaskInfo));
    }

    @Test
    public void shouldKillTheTask() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = killTaskWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        String info = "Dropped by the user";
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));
        Response detailedReportResponse = enrichedWebTarget.request().post(null);
        assertEquals(200, detailedReportResponse.getStatus());
        assertEquals(detailedReportResponse.readEntity(String.class), "The task was killed because of " + info);
    }


    @Test
    public void shouldKillTheTaskWhenPassingTheCauseOfKilling() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = killTaskWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        String info = "The aggregator decided to do so";
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));
        Response detailedReportResponse = enrichedWebTarget.queryParam("info", info).request().post(null);
        assertEquals(200, detailedReportResponse.getStatus());
        assertEquals(detailedReportResponse.readEntity(String.class), "The task was killed because of " + info);
    }


    @Test
    public void killTaskShouldFailForNonExistedTopology() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = killTaskWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        String info = "Dropped by the user";
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(false);
        Response detailedReportResponse = enrichedWebTarget.request().post(null);
        assertEquals(405, detailedReportResponse.getStatus());
    }

    @Test
    public void killTaskShouldFailWhenTaskDoesNotBelongToTopology() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = killTaskWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        String info = "Dropped by the user";
        doNothing().when(killService).killTask(eq(TASK_ID), eq(info));
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        Response detailedReportResponse = enrichedWebTarget.request().post(null);
        assertEquals(405, detailedReportResponse.getStatus());
    }

    @Test
    public void shouldGetGeneralErrorReportWithIdentifiers() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = errorsReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID).queryParam("idsCount", 10);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
        when(reportService.getGeneralTaskErrorReport(eq(Long.toString(TASK_ID)), eq(10))).thenReturn(errorsInfo);

        Response response = enrichedWebTarget.request().get();
        TaskErrorsInfo retrievedInfo = response.readEntity(TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }


    @Test
    public void shouldCheckIfReportExists() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = errorsReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        when(reportService.checkIfReportExists(eq(Long.toString(TASK_ID)))).thenReturn(true);
        Response response = enrichedWebTarget.request().head();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void shouldReturn405InCaseOfException() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = errorsReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        doThrow(new AccessDeniedOrObjectDoesNotExistException()).when(reportService).checkIfReportExists(eq(Long.toString(TASK_ID)));
        Response response = enrichedWebTarget.request().head();
        assertEquals(405, response.getStatus());

    }

    @Test
    public void shouldGetSpecificErrorReport() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = errorsReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID).queryParam("error", ERROR_TYPES[0]);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(true);
        when(reportService.getSpecificTaskErrorReport(eq(Long.toString(TASK_ID)), eq(ERROR_TYPES[0]), eq(100))).thenReturn(errorsInfo);

        Response response = enrichedWebTarget.request().get();
        TaskErrorsInfo retrievedInfo = response.readEntity(TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }

    @Test
    public void shouldGetGeneralErrorReport() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = errorsReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        TaskErrorsInfo errorsInfo = createDummyErrorsInfo(false);
        when(reportService.getGeneralTaskErrorReport(eq(Long.toString(TASK_ID)), eq(0))).thenReturn(errorsInfo);

        Response response = enrichedWebTarget.request().get();
        TaskErrorsInfo retrievedInfo = response.readEntity(TaskErrorsInfo.class);
        assertThat(retrievedInfo, is(errorsInfo));
    }


    @Test
    public void shouldThrowExceptionIfTaskIdWasNotFound() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = progressReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        when(reportService.getTaskProgress(eq(Long.toString(TASK_ID)))).thenThrow(AccessDeniedOrObjectDoesNotExistException.class);
        when(topologyManager.containsTopology(TOPOLOGY_NAME)).thenReturn(true);
        Response detailedReportResponse = enrichedWebTarget.request().get();
        assertEquals(405, detailedReportResponse.getStatus());
    }

    @Test
    public void shouldGetDetailedReportForSpecifiedResources() throws AccessDeniedOrObjectDoesNotExistException {
        WebTarget enrichedWebTarget = detailedReportWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, TOPOLOGY_NAME).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID).queryParam("from", 120).queryParam("to", 150);
        List<SubTaskInfo> subTaskInfoList = createDummySubTaskInfoList();
        when(reportService.getDetailedTaskReportBetweenChunks(eq(Long.toString(TASK_ID)), eq(120), eq(150))).thenReturn(subTaskInfoList);
        when(topologyManager.containsTopology(anyString())).thenReturn(true);
        doNothing().when(reportService).checkIfTaskExists(eq(Long.toString(TASK_ID)), eq(TOPOLOGY_NAME));
        Response detailedReportResponse = enrichedWebTarget.request().get();
        assertDetailedReportResponse(subTaskInfoList.get(0), detailedReportResponse);
    }


    @Test
    public void shouldProperlySendTaskWithDataSetEntryAndRevisionToLinkCheckTopology() throws MCSException, TaskSubmissionException, InterruptedException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        prepareTaskWithRepresentationAndRevision(task);
        prepareMocks(LINK_CHECKING_TOPOLOGY);

        Response response = sendTask(task, LINK_CHECKING_TOPOLOGY);
        assertSuccessfulRequest(response, LINK_CHECKING_TOPOLOGY);
    }

    @Test
    public void shouldProperlySendTaskWithDataSetEntryWithoutRevisionToLinkCheckTopology() throws MCSException, TaskSubmissionException, InterruptedException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        task.addParameter(REPRESENTATION_NAME, REPRESENTATION_NAME);

        prepareMocks(LINK_CHECKING_TOPOLOGY);
        Response response = sendTask(task, LINK_CHECKING_TOPOLOGY);

        assertSuccessfulRequest(response, LINK_CHECKING_TOPOLOGY);
    }

    @Test
    public void shouldThrowValidationExceptionWhenSendingTaskToLinkCheckWithWrongDataSetURL() throws MCSException, TaskSubmissionException {
        DpsTask task = new DpsTask(TASK_NAME);
        task.addDataEntry(DATASET_URLS, Arrays.asList(WRONG_DATA_SET_URL));
        prepareMocks(LINK_CHECKING_TOPOLOGY);

        Response response = sendTask(task, LINK_CHECKING_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowValidationExceptionWhenSendingTaskToLinkCheckTopologyWithNotValidOutputRevision() throws MCSException, TaskSubmissionException {
        DpsTask task = getDpsTaskWithDataSetEntry();
        Revision revision = new Revision(" ", REVISION_PROVIDER);
        task.setOutputRevision(revision);
        prepareMocks(LINK_CHECKING_TOPOLOGY);

        Response response = sendTask(task, LINK_CHECKING_TOPOLOGY);

        assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldProperlyHandleCleaningRequest() throws MCSException, TaskSubmissionException, InterruptedException {
        DataSetCleanerParameters dataSetCleanerParameters = prepareDataSetCleanerParameters();
        mockSecurity(INDEXING_TOPOLOGY);
        WebTarget enrichedWebTarget = cleanDatasetWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, INDEXING_TOPOLOGY).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        Response response = enrichedWebTarget.request().post(Entity.entity(dataSetCleanerParameters, MediaType.APPLICATION_JSON_TYPE));
        assertNotNull(response);
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        Thread.sleep(1000);
        verify(taskDAO, times(1)).setTaskStatus(eq(TASK_ID), eq("Completely process"), eq(TaskState.PROCESSED.toString()));
        verifyNoMoreInteractions(taskDAO);
    }

    @Test
    public void shouldThrowAccessDeniedWithNoCredentials() throws MCSException, TaskSubmissionException, InterruptedException {
        DataSetCleanerParameters dataSetCleanerParameters = prepareDataSetCleanerParameters();
        WebTarget enrichedWebTarget = cleanDatasetWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, INDEXING_TOPOLOGY).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        Response response = enrichedWebTarget.request().post(Entity.entity(dataSetCleanerParameters, MediaType.APPLICATION_JSON_TYPE));
        assertNotNull(response);
        assertThat(response.getStatus(), is(Response.Status.METHOD_NOT_ALLOWED.getStatusCode()));

    }

    @Test
    public void shouldDropTaskWhenCleanerParametersAreNull() throws MCSException, TaskSubmissionException, InterruptedException {
        mockSecurity(INDEXING_TOPOLOGY);
        WebTarget enrichedWebTarget = cleanDatasetWebTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, INDEXING_TOPOLOGY).resolveTemplate(TASK_ID_PARAMETER_LABEL, TASK_ID);
        Response response = enrichedWebTarget.request().post(null);
        assertNotNull(response);
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        Thread.sleep(1000);
        verify(taskDAO, times(1)).dropTask(eq(TASK_ID), eq("cleaner parameters can not be null"), eq(TaskState.DROPPED.toString()));
        verifyNoMoreInteractions(taskDAO);

    }

    private void assertSuccessfulRequest(Response response, String topologyName) throws InterruptedException {
        assertNotNull(response);
        assertThat(response.getStatus(), is(Response.Status.CREATED.getStatusCode()));
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

    private Response sendTask(DpsTask task, String topologyName) {
        WebTarget enrichedWebTarget = webTarget.resolveTemplate(TOPOLOGY_NAME_PARAMETER_LABEL, topologyName);
        return enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

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
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, RESOURCE_URL, States.SUCCESS, EMPTY_STRING, EMPTY_STRING, RESULT_RESOURCE_URL);
        subTaskInfoList.add(subTaskInfo);
        return subTaskInfoList;
    }

    private void assertDetailedReportResponse(SubTaskInfo subTaskInfo, Response detailedReportResponse) {
        assertThat(detailedReportResponse.getStatus(), is(Response.Status.OK.getStatusCode()));
        List<SubTaskInfo> resultedSubTaskInfoList = detailedReportResponse.readEntity(new GenericType<List<SubTaskInfo>>() {
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