package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.ApplicationContextUtils;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.kafka.KafkaSubmitService;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import static eu.europeana.cloud.service.dps.InputDataType.*;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;


public class TopologyTasksResourceTest extends JerseyTest {
    private TopologyManager topologyManager;
    private MutableAclService mutableAclService;
    private WebTarget webTarget;
    private RecordServiceClient recordServiceClient;
    private ApplicationContext context;
    private DataSetServiceClient dataSetServiceClient;
    private FileServiceClient fileServiceClient;
    private CassandraTaskInfoDAO taskDAO;
    private FilesCounterFactory filesCounterFactory;
    private FilesCounter filesCounter;
    private KafkaSubmitService kafkaSubmitService;


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
        dataSetServiceClient = applicationContext.getBean(DataSetServiceClient.class);
        fileServiceClient = applicationContext.getBean(FileServiceClient.class);
        taskDAO = applicationContext.getBean(CassandraTaskInfoDAO.class);
        kafkaSubmitService = applicationContext.getBean(KafkaSubmitService.class);
        webTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value());

    }

    @Test
    public void shouldProperlySendTask() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        String topologyName = "ic_topology";
        prepareMocks(topologyName);
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
    }

    @Test
    public void shouldProperlySendTaskWithDataSetEntry() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        String topologyName = "ic_topology";
        prepareMocks(topologyName);
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
    }

    @Test
    public void shouldProperlySendTaskWithOaiPmhRepository() throws MCSException, TaskSubmissionException, InterruptedException {
        //given
        DpsTask task = new DpsTask("oaiPmhHarvestingTask");
        task.addDataEntry(REPOSITORY_URLS, Arrays.asList
                ("http://example.com/oai-pmh-repository.xml"));
        task.setHarvestingDetails(new OAIPMHHarvestingDetails("Schema"));
        String topologyName = "oai_pmh_topology";
        prepareMocks(topologyName);
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
        Thread.sleep(10000);
        verify(kafkaSubmitService).submitTask(any(DpsTask.class), eq(topologyName));
        verifyNoMoreInteractions(kafkaSubmitService);
    }

    @Test
    public void shouldProperlySendTaskWithDatsetEntryWithOutputRevision() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        task.setOutputRevision(new Revision("REVISION_NAME", "REVISION_PROVIDER"));
        String topologyName = "ic_topology";
        prepareMocks(topologyName);
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionWhenMissingRepresentationName() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        String topologyName = "ic_topology";
        prepareMocks(topologyName);

        //when
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //then
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }


    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSendTask() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        String topologyName = "ic_topology";
        prepareMocks(topologyName);

        //when
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //then
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision_1() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        task.setOutputRevision(new Revision("", "REVISION_PROVIDER"));
        String topologyName = "ic_topology";
        prepareMocks(topologyName);
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision_2() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        task.setOutputRevision(new Revision("", ""));
        String topologyName = "ic_topology";
        prepareMocks(topologyName);
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    @Test
    public void shouldThrowExceptionOnSendTaskWithMalformedOutputRevision_3() throws MCSException, TaskSubmissionException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
        task.addParameter(OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(MIME_TYPE, "image/tiff");
        task.addParameter(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        task.setOutputRevision(new Revision(null, null));
        String topologyName = "ic_topology";
        prepareMocks(topologyName);
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //when
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));

        //then
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    private void prepareMocks(String topologyName) throws MCSException, TaskSubmissionException {
        //Mock security
        HashMap<String, String> user = new HashMap<>();
        user.put(topologyName, "Smith");
        MutableAcl mutableAcl = mock(MutableAcl.class);
        //Mock
        when(topologyManager.containsTopology(topologyName)).thenReturn(true);
        when(mutableAcl.getEntries()).thenReturn(Collections.EMPTY_LIST);
        doNothing().when(mutableAcl).insertAce(anyInt(), any(Permission.class), any(Sid.class), anyBoolean());
        doNothing().when(taskDAO).insert(anyLong(), anyString(), anyInt(), anyString(), anyString(), isA(Date.class));
        when(mutableAclService.readAclById(any(ObjectIdentity.class))).thenReturn(mutableAcl);
        when(context.getBean(RecordServiceClient.class)).thenReturn(recordServiceClient);
        when(context.getBean(FileServiceClient.class)).thenReturn(fileServiceClient);
        when(context.getBean(DataSetServiceClient.class)).thenReturn(dataSetServiceClient);
        when(filesCounterFactory.createFilesCounter(anyString())).thenReturn(filesCounter);
        when(filesCounter.getFilesCount(isA(DpsTask.class), anyString())).thenReturn(1);
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(eu.europeana.cloud.common.model.Permission.class));
    }

}