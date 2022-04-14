package eu.europeana.cloud.service.uis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
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

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * DataProviderResourceTest
 */
@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {TestConfiguration.class})
public class DataProviderResourceTest {

    MockMvc mockMvc;
    @Autowired
    private DataProviderService dataProviderService;
    @Autowired
    private WebApplicationContext wac;
    @Autowired
    private UniqueIdentifierService uniqueIdentifierService;

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

    @Before
    public void mockUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    private String toJson(Object object) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writer().writeValueAsString(object);
    }

    private ErrorInfo readErrorInfoFromResponse(String response) throws IOException {
        return new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                response, ErrorInfo.class);
    }

    @Test
    public void shouldUpdateProvider() throws Exception {

        // given certain provider data
        String providerName = "provider";

        DataProvider dp = new DataProvider();
        dp.setId(providerName);
        // when the provider is updated
        DataProviderProperties properties = new DataProviderProperties();
        properties.setOrganisationName("Organizacja");
        properties.setRemarks("Remarks");
        dp.setProperties(properties);
        Mockito.doReturn(dp).when(dataProviderService).updateProvider(providerName, properties);

        mockMvc.perform(put(RestInterfaceConstants.DATA_PROVIDER, providerName)
                .content(toJson(properties)).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNoContent());

        dp.setProperties(properties);

        Mockito.doReturn(dp).when(dataProviderService).getProvider(providerName);

        // then the inserted provider should be in service
        DataProvider retProvider = dataProviderService
                .getProvider(providerName);

        assertEquals(providerName, retProvider.getId());
        assertEquals(properties, retProvider.getProperties());

    }

    @Test
    public void shouldGetProvider() throws Exception {
        // given certain provider in service
        DataProviderProperties properties = new DataProviderProperties();
        properties.setOrganisationName("Organizacja");
        properties.setRemarks("Remarks");
        String providerName = "provident";

        DataProvider dp = new DataProvider();
        dp.setId(providerName);

        dp.setProperties(properties);
        Mockito.doReturn(dp).when(dataProviderService).getProvider(providerName);

        // when you get provider by rest api
        MvcResult mvcResult = mockMvc.perform(get(RestInterfaceConstants.DATA_PROVIDER, providerName).accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        DataProvider receivedDataProvider = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, DataProvider.class);

        // then received provider should be the same as inserted
        assertEquals(providerName, receivedDataProvider.getId());
        assertEquals(properties, receivedDataProvider.getProperties());
    }

    @Test
    public void shouldReturn404OnNotExistingProvider()
            throws Exception {
        // given there is no provider in service
        Mockito.doThrow(
                new ProviderDoesNotExistException(new IdentifierErrorInfo(
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getHttpCode(),
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getErrorInfo("provident")))
        ).when(dataProviderService).getProvider("provident");

        MvcResult mvcResult = mockMvc.perform(get(RestInterfaceConstants.DATA_PROVIDER, "provident").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        ErrorInfo deleteErrorInfo = readErrorInfoFromResponse(mvcResult.getResponse().getContentAsString());

        assertEquals(IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                        .getErrorInfo("provident").getErrorCode(),
                deleteErrorInfo.getErrorCode());
    }

    @Test
    public void shouldDeleteProvider() throws Exception {
        mockMvc.perform(delete(RestInterfaceConstants.DATA_PROVIDER, "provident").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void testCreateMapping() throws Exception {
        CloudId gid = createCloudId("providerId", "recordId");

        Mockito.doReturn(gid).when(uniqueIdentifierService).createCloudId("providerId", "recordId");
        Mockito.doReturn(gid).when(uniqueIdentifierService).createIdMapping(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(gid).when(uniqueIdentifierService).createIdMapping(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(post(RestInterfaceConstants.CLOUD_IDS)
                .param(UISParamConstants.Q_PROVIDER_ID, "providerId")
                .param(UISParamConstants.Q_RECORD_ID, "recordId")
                .accept(MediaType.APPLICATION_JSON));

        mockMvc.perform(post(RestInterfaceConstants.CLOUD_ID_TO_RECORD_ID_MAPPING, "providerId", gid.getId())
                .param(UISParamConstants.Q_RECORD_ID, "local1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }

    @Test
    public void testCreateMappingDBException() throws Exception {
        Throwable exception = new DatabaseConnectionException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getHttpCode(),
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getErrorInfo(
                                        uniqueIdentifierService.getHostList(),
                                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).createIdMapping(
                "cloudId", "providerId", "local1");

        MvcResult mvcResult = mockMvc.perform(post(RestInterfaceConstants.CLOUD_ID_TO_RECORD_ID_MAPPING, "providerId", "cloudId")
                .param(UISParamConstants.Q_RECORD_ID, "local1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        ErrorInfo errorInfo = readErrorInfoFromResponse(mvcResult.getResponse().getContentAsString());

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
                        uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "")
                        .getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                        .getErrorInfo(
                                uniqueIdentifierService.getHostList(),
                                uniqueIdentifierService.getHostList(),
                                "").getDetails());
    }

    @Test
    public void testCreateMappingCloudIdException() throws Exception {
        Throwable exception = new CloudIdDoesNotExistException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
                                .getHttpCode(),
                        IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
                                .getErrorInfo("cloudId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).createIdMapping(
                "cloudId", "providerId", "local1");

        MvcResult mvcResult = mockMvc.perform(post(RestInterfaceConstants.CLOUD_ID_TO_RECORD_ID_MAPPING, "providerId", "cloudId")
                .param(UISParamConstants.Q_RECORD_ID, "local1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        ErrorInfo errorInfo = readErrorInfoFromResponse(mvcResult.getResponse().getContentAsString());

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(
                        "cloudId").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(
                        "cloudId").getDetails());
    }

    @Test
    public void testRemoveMapping() throws Exception {
        CloudId gid = createCloudId("providerId", "recordId");
        Mockito.reset(uniqueIdentifierService);
        Mockito.when(uniqueIdentifierService.createCloudId("providerId", "recordId")).thenReturn(gid);

        mockMvc.perform(get(RestInterfaceConstants.CLOUD_IDS)
                .param("providerId", "providerId")
                .param(UISParamConstants.Q_RECORD_ID, "recordId")
                .accept(MediaType.APPLICATION_JSON));

        mockMvc.perform(delete(RestInterfaceConstants.RECORD_ID_MAPPING_REMOVAL, "providerId", "recordId"))
                .andExpect(status().isOk());
    }

    @Test
    public void testRemoveMappingDBException() throws Exception {
        Throwable exception = new DatabaseConnectionException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getHttpCode(),
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getErrorInfo(
                                        uniqueIdentifierService.getHostList(),
                                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).removeIdMapping(
                "providerId", "recordId");

        MvcResult mvcResult = mockMvc.perform(delete(RestInterfaceConstants.RECORD_ID_MAPPING_REMOVAL, "providerId", "recordId").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        ErrorInfo errorInfo = readErrorInfoFromResponse(mvcResult.getResponse().getContentAsString());

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
                        uniqueIdentifierService.getHostList(),
                        uniqueIdentifierService.getHostList(), "")
                        .getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                        .getErrorInfo(
                                uniqueIdentifierService.getHostList(),
                                uniqueIdentifierService.getHostList(),
                                "").getDetails());
    }

    @Test
    public void testRemoveMappingProviderDoesNotExistException()
            throws Exception {
        Throwable exception = new ProviderDoesNotExistException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getHttpCode(),
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getErrorInfo("providerId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).removeIdMapping(
                "providerId", "recordId");

        MvcResult mvcResult = mockMvc.perform(delete(RestInterfaceConstants.RECORD_ID_MAPPING_REMOVAL, "providerId", "recordId").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        ErrorInfo errorInfo = readErrorInfoFromResponse(mvcResult.getResponse().getContentAsString());

        assertEquals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getDetails());
    }
}
