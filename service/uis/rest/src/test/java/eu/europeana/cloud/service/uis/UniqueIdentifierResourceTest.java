package eu.europeana.cloud.service.uis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.*;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * UniqueIdResource unit test
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 23, 2013
 */
@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {TestConfiguration.class})
public class UniqueIdentifierResourceTest {

    private final String providerId = "providerId";
    private final String recordId = "recordId";

    @Autowired
    private UniqueIdentifierService uniqueIdentifierService;

    MockMvc mockMvc;

    @Autowired
    private WebApplicationContext wac;

    @Before
    public void mockUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @Test
    public void testCreateCloudId()
            throws Exception {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        Mockito.reset(uniqueIdentifierService);
        CloudId originalGid = createCloudId(providerId, recordId);
        Mockito.when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(originalGid);

        MvcResult mvcResult = mockMvc.perform(post("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        CloudId retrieveCreate = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, CloudId.class);

        assertEquals(originalGid.getId(), retrieveCreate.getId());
        assertEquals(originalGid.getLocalId().getProviderId(), retrieveCreate.getLocalId().getProviderId());
        assertEquals(originalGid.getLocalId().getRecordId(), retrieveCreate.getLocalId().getRecordId());
    }

    @Test
    public void testCreateCloudIdDbException()
            throws Exception {
        Mockito.reset(uniqueIdentifierService);

        Throwable databaseException = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(
                databaseException).when(uniqueIdentifierService).createCloudId(providerId, recordId);

        MvcResult mvcResult = mockMvc.perform(post("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }

    @Test
    public void testCreateCloudIdRecordExistsException()
            throws Exception {
        Throwable exception = new RecordExistsException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.RECORD_EXISTS.getHttpCode(),
                IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId)));

        Mockito.doThrow(exception).when(uniqueIdentifierService).createCloudId(providerId, recordId);

        MvcResult mvcResult = mockMvc.perform(post("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId).getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId).getDetails());
    }

    @Test
    public void testCreateCloudIdAlreadyExistException()
            throws Exception {
        // given
        Throwable exception = new CloudIdAlreadyExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getHttpCode(),
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(providerId, recordId)));

        Mockito.doThrow(exception).when(uniqueIdentifierService).createCloudId(providerId, recordId);

        MvcResult mvcResult = mockMvc.perform(post("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(providerId, recordId).getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(providerId, recordId).getDetails());
    }

    @Test
    public void testGetCloudId()
            throws Exception {
        CloudId originalGid = createCloudId(providerId, recordId);

        Mockito.doReturn(originalGid).when(uniqueIdentifierService).getCloudId(providerId, recordId);

        MvcResult mvcResult = mockMvc.perform(get("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        CloudId retrieveGet = new ObjectMapper().readValue(
                content, CloudId.class);

        assertEquals(originalGid.getId(), retrieveGet.getId());
        assertEquals(originalGid.getLocalId().getProviderId(), retrieveGet.getLocalId().getProviderId());
        assertEquals(originalGid.getLocalId().getRecordId(), retrieveGet.getLocalId().getRecordId());
    }

    @Test
    public void testGetCloudIdDBException()
            throws Exception {
        Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getCloudId(providerId, recordId);

        MvcResult mvcResult = mockMvc.perform(get("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }

    @Test
    public void testGetCloudIdRecordDoesNotExistException()
            throws Exception {
        Throwable exception = new RecordDoesNotExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getHttpCode(),
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId)));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getCloudId(providerId, recordId);

        MvcResult mvcResult = mockMvc.perform(get("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getDetails());
    }

    @Test
    public void testGetLocalIds()
            throws Exception {
        ResultSlice<CloudId> lidListWrapper = new ResultSlice<>();
        List<CloudId> localIdList = new ArrayList<>(1);
        localIdList.add(createCloudId(providerId, recordId));
        String cloudId = createCloudId(providerId, recordId).getId();
        lidListWrapper.setResults(localIdList);

        Mockito.doReturn(localIdList).when(uniqueIdentifierService).getLocalIdsByCloudId(cloudId);

        MvcResult mvcResult = mockMvc.perform(get("/cloudIds/" + cloudId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ResultSlice<CloudId> retList = new ObjectMapper().readValue(
                content, new TypeReference<ResultSlice<CloudId>>() {
                });

        assertThat(retList.getResults().size(), is(lidListWrapper.getResults().size()));
        assertEquals(retList.getResults().get(0).getLocalId().getProviderId(), lidListWrapper.getResults().get(0)
                .getLocalId().getProviderId());
        assertEquals(retList.getResults().get(0).getLocalId().getRecordId(), lidListWrapper.getResults().get(0)
                .getLocalId().getRecordId());
    }

    @Test
    public void testGetLocalIdsDBException()
            throws Exception {
        Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getLocalIdsByCloudId("cloudId");

        MvcResult mvcResult = mockMvc.perform(get("/cloudIds/cloudId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }

    @Test
    public void testGetLocalIdsCloudIdDoesNotExistException()
            throws Exception {
        Throwable exception = new CloudIdDoesNotExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getLocalIdsByCloudId("cloudId");

        MvcResult mvcResult = mockMvc.perform(get("/cloudIds/cloudId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getDetails());
    }

    @Test
    public void testDeleteCloudId()
            throws Exception {
        CloudId gid = createCloudId(providerId, recordId);

        Mockito.doReturn(gid).when(uniqueIdentifierService).createCloudId(providerId, recordId);

        mockMvc.perform(post("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON));

        mockMvc.perform(delete("/cloudIds/"+gid.getId())
                .param(UISParamConstants.Q_PROVIDER_ID, providerId)
                .param(UISParamConstants.Q_RECORD_ID, recordId)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }

    @Test
    public void testDeleteCloudIdDBException()
            throws Exception {
        Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

        MvcResult mvcResult = mockMvc.perform(delete("/cloudIds/cloudId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "").getDetails());
    }

    @Test
    public void testDeleteCloudIdCloudIdDoesNotExistException()
            throws Exception {
        Throwable exception = new CloudIdDoesNotExistException(new IdentifierErrorInfo(
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

        MvcResult mvcResult = mockMvc.perform(delete("/cloudIds/cloudId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getDetails());
    }


    private static LocalId createLocalId(String providerId, String recordId) {
        LocalId localId = new LocalId();
        localId.setProviderId(providerId);
        localId.setRecordId(recordId);
        return localId;
    }


    private static CloudId createCloudId(String providerId, String recordId) {
        CloudId cloudId = new CloudId();
        cloudId.setLocalId(createLocalId(providerId, recordId));
        cloudId.setId(IdGenerator.encodeWithSha256AndBase32("/" + providerId + "/" + recordId));
        return cloudId;
    }
}
