package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.ApplicationContextUtils;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.model.*;
import org.springframework.security.acls.model.Permission;

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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;


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
        context = applicationContext.getBean(ApplicationContext.class);
        dataSetServiceClient = applicationContext.getBean(DataSetServiceClient.class);
        fileServiceClient = applicationContext.getBean(FileServiceClient.class);
        taskDAO = applicationContext.getBean(CassandraTaskInfoDAO.class);
        webTarget = target(TopologyTasksResource.class.getAnnotation(Path.class).value());

    }

    @Test
    public void shouldProperlySendTask() throws MCSException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        String topologyName = "ic_topology";
        prepareMocks(topologyName);

        //when
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //then
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
    }

    @Test
    public void shouldProperlySendTaskWithDatsetEntry() throws MCSException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DpsTask.DATASET_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/data-providers/stormTestTopologyProvider/data-sets/tiffDataSets"));
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        String topologyName = "ic_topology";
        prepareMocks(topologyName);

        //when
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //then
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.CREATED.getStatusCode()));
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSendTask() throws MCSException {
        //given
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        String topologyName = "ic_topology";
        prepareMocks(topologyName);

        //when
        WebTarget enrichedWebTarget = webTarget.resolveTemplate("topologyName", topologyName);

        //then
        Response sendTaskResponse = enrichedWebTarget.request().post(Entity.entity(task, MediaType.APPLICATION_JSON_TYPE));
        assertThat(sendTaskResponse.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
    }

    private void prepareMocks(String topologyName) throws MCSException {
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
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(dataSetServiceClient).useAuthorizationHeader(anyString());
        doNothing().when(recordServiceClient).grantPermissionsToVersion(anyString(), anyString(), anyString(), anyString(), any(eu.europeana.cloud.common.model.Permission.class));
    }

}