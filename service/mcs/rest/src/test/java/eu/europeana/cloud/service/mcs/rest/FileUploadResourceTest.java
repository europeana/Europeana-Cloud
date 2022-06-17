package eu.europeana.cloud.service.mcs.rest;

import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestInstance;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(CassandraTestRunner.class)
public class FileUploadResourceTest extends CassandraBasedAbstractResourceTest {

    private String fileWebTarget;

    private File file;

    private static final UUID VERSION = UUID.fromString(new com.eaio.uuid.UUID().toString());

    @Autowired
    private RecordService recordService;

    @Before
    public void init(){
        CassandraTestInstance.truncateAllData(false);
        Mockito.reset(recordService);
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

        fileWebTarget = "/records/"+rep.getCloudId()+"/representations/"+rep.getRepresentationName()+"/files";
    }
    
    @Test
    public void shouldUploadFileForNonExistingRepresentation() throws Exception {
        //given
        String providerId = "providerId";
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        //when
        mockMvc.perform(postFile(fileWebTarget, file.getMimeType(), content)
                .param(ParamConstants.F_FILE_NAME, file.getFileName())
                .param(ParamConstants.F_PROVIDER, providerId))
                //then
                .andExpect(status().isCreated())
                .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));
    }


    @Test
    public void shouldUploadFileInGivenVersionForNonExistingRepresentation() throws Exception {
        //given
        String providerId = "providerId";
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        //when
        mockMvc.perform(postFile(fileWebTarget, file.getMimeType(), content)
                .param(ParamConstants.F_FILE_NAME, file.getFileName())
                .param(ParamConstants.F_PROVIDER, providerId)
                .param(ParamConstants.VERSION, VERSION.toString()))
                //then
                .andExpect(status().isCreated())
                .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));

        verify(recordService).createRepresentation(any(), any(), any(), eq(VERSION), any());
    }

    @Test
    public void shouldAllowUploadFileInGivenVersionTwice() throws Exception {
        //given
        String providerId = "providerId";
        byte[] content = new byte[1000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();
        //when
        mockMvc.perform(postFile(fileWebTarget, file.getMimeType(), content)
                .param(ParamConstants.F_FILE_NAME, file.getFileName())
                .param(ParamConstants.F_PROVIDER, providerId)
                .param(ParamConstants.VERSION, VERSION.toString()))
                //then
                .andExpect(status().isCreated())
                .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));

        mockMvc.perform(postFile(fileWebTarget, file.getMimeType(), content)
                .param(ParamConstants.F_FILE_NAME, file.getFileName())
                .param(ParamConstants.F_PROVIDER, providerId)
                .param(ParamConstants.VERSION, VERSION.toString()))
                //then
                .andExpect(status().isCreated())
                .andExpect(header().string(HttpHeaders.ETAG, isEtag(contentMd5)));

        verify(recordService, times(2)).createRepresentation(any(), any(), any(), eq(VERSION), any());
    }

}

