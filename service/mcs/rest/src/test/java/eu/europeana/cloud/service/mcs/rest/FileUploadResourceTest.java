package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestInstance;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockMultipartFile;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(CassandraTestRunner.class)
public class FileUploadResourceTest extends CassandraBasedAbstractResourceTest {

    private String fileWebTarget;

    private File file;


    @Before
    public void init(){
        CassandraTestInstance.truncateAllData(false);
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

        fileWebTarget = "/records/{"+rep.getCloudId()+"/representations/"+rep.getRepresentationName()+"/files";
    }
    
    @Test
    public void shouldUploadFileForNonExistingRepresentation() throws Exception {
        //given
        String providerId = "providerId";
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        MockMultipartFile multipart= new MockMultipartFile(file.getFileName(),null, file.getMimeType(),content);
        //when
        mockMvc.perform(putMultipart(fileWebTarget).file(multipart)
                .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM))
        //then
                .andExpect(status().isUnauthorized())
                .andExpect(header().string(HttpHeaders.ETAG,contentMd5));
    }



}

