package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestInstance;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(CassandraTestRunner.class)
public class FileUploadResourceTest extends JerseyTest {

    private WebTarget fileWebTarget;

    private File file;

    @Override
    public Application configure() {
        return null; //new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }
    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
    }

    @Before
    public void init(){
        CassandraTestInstance.truncateAllData(false);
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        UISClientHandler uisHandler = applicationContext.getBean(UISClientHandler.class);
        Mockito.doReturn(new DataProvider()).when(uisHandler).getProvider(Mockito.anyString());
        Mockito.doReturn(true).when(uisHandler).existsCloudId(Mockito.anyString());
        Representation rep = new Representation();
        rep.setCloudId("cloudId");
        rep.setRepresentationName("representationName");
        rep.setVersion("versionId");
        file = new File();
        file.setFileName("fileName");
        file.setMimeType("application/octet-stream");
        Map<String, Object> allPathParams = ImmutableMap.<String, Object>of(
                CLOUD_ID, rep.getCloudId(),
                REPRESENTATION_NAME, rep.getRepresentationName(),
                VERSION, rep.getVersion());

        fileWebTarget = target(FileUploadResource.class.getAnnotation(Path.class).value()).resolveTemplates(allPathParams);
    }
    
    @Test
    public void shouldUploadFileForNonExistingRepresentation()  {
        //given
        String providerId = "providerId";
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        FormDataMultiPart multipart = new FormDataMultiPart()
                .field(ParamConstants.F_PROVIDER,providerId)
                .field(ParamConstants.F_FILE_MIME, file.getMimeType())
                .field(ParamConstants.F_FILE_DATA, new ByteArrayInputStream(content),
                        MediaType.APPLICATION_OCTET_STREAM_TYPE).field(ParamConstants.F_FILE_NAME, file.getFileName());
        //when
        Response uploadFileResponse = fileWebTarget.request().post(Entity.entity(multipart, multipart.getMediaType()));
        //then
        assertThat("Unexpected status code",uploadFileResponse.getStatus(),is(201));
        assertThat("File content tag does not match",uploadFileResponse.getEntityTag().getValue(),is(contentMd5));
    }



}

