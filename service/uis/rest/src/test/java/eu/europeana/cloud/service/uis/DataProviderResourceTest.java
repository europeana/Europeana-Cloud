package eu.europeana.cloud.service.uis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.*;
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
import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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

        ObjectMapper mapper = new ObjectMapper();
        String requestJson = mapper.writer().writeValueAsString(properties);

        mockMvc.perform(put("/data-providers/{" + P_PROVIDER + "}", providerName)
                .content(requestJson).contentType(MediaType.APPLICATION_JSON))
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
        MvcResult mvcResult = mockMvc.perform(get("/data-providers/{" + P_PROVIDER + "}", providerName).accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        DataProvider receivedDataProvider = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, DataProvider.class);

        // then received provider should be the same as inserted
        assertEquals(providerName, receivedDataProvider.getId());
        assertEquals(properties, receivedDataProvider.getProperties());
    }

    /**
     * Test Non Existing provider
     */
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

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/{" + P_PROVIDER + "}", "provident").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo deleteErrorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                        .getErrorInfo("provident").getErrorCode(),
                deleteErrorInfo.getErrorCode());
    }

    @Test
    public void shouldDeleteProvider() throws Exception {
        mockMvc.perform(delete("/data-providers/{" + P_PROVIDER + "}", "provident").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    /**
     * Test the retrieval of local Ids by a provider
     */
    @Test
    public void testGetLocalIdsByProvider()
            throws Exception {
        ResultSlice<CloudId> lidListWrapper = new ResultSlice<>();
        List<CloudId> localIdList = new ArrayList<>();
        localIdList.add(createCloudId("providerId", "recordId"));
        lidListWrapper.setResults(localIdList);
        Mockito.doReturn(localIdList).when(uniqueIdentifierService).getLocalIdsByProvider("providerId", "recordId", 10000);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/localIds").param("from", "recordId").accept(MediaType.APPLICATION_JSON))
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

    /**
     * Test the database exception
     */
    @Test
    public void testGetLocalIdsByProviderDBException() throws Exception {
        Throwable exception = new DatabaseConnectionException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getHttpCode(),
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getErrorInfo(
                                        uniqueIdentifierService.getHostList(),
                                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getLocalIdsByProvider("providerId",
                "recordId", 10000);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/localIds").param("from", "recordId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

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

    /**
     * The the retrieval of cloud ids based on a provider with specified limit
     */
    @Test
    public void testGetCloudIdsByProviderWithLimit()
            throws Exception {
        //given
        final int limit = 1;
        ResultSlice<CloudId> resultSlice = new ResultSlice<>();
        CloudId cloudId = createCloudId("providerId", "recordId");
        CloudId nextSliceCloudId = createCloudId("providerId", "recordId2");
        List<CloudId> cloudIds = new ArrayList<>(Arrays.asList(cloudId, nextSliceCloudId));
        resultSlice.setResults(cloudIds);

        Mockito.doReturn(cloudIds).when(uniqueIdentifierService).getCloudIdsByProvider("providerId", "recordId", 2);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/cloudIds")
                .param(UISParamConstants.Q_FROM, "recordId")
                .param(UISParamConstants.Q_LIMIT, String.valueOf(limit))
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ResultSlice<CloudId> result = new ObjectMapper().readValue(
                content, new TypeReference<ResultSlice<CloudId>>() {
                });

        assertThat(result.getResults().size(), is(limit));
        CloudId firstEntryFromResultSlice = result.getResults().get(0);
        assertEquals(firstEntryFromResultSlice.getId(), cloudId.getId());
        assertEquals(firstEntryFromResultSlice.getLocalId().getProviderId(), cloudId
                .getLocalId().getProviderId());
        assertEquals(firstEntryFromResultSlice.getLocalId().getRecordId(), cloudId
                .getLocalId().getRecordId());
        assertThat(result.getNextSlice(), is(nextSliceCloudId.getLocalId().getRecordId()));
    }

    /**
     * The the retrieval of cloud ids based on a provider with less than specified limit
     */
    @Test
    public void testGetCloudIdsByProviderWithLessThanLimit()
            throws Exception {
        //given
        final int limit = 3;
        ResultSlice<CloudId> resultSlice = new ResultSlice<>();
        CloudId cloudId = createCloudId("providerId", "recordId");
        CloudId nextSliceCloudId = createCloudId("providerId", "recordId2");
        List<CloudId> cloudIds = new ArrayList<>(Arrays.asList(cloudId, nextSliceCloudId));
        resultSlice.setResults(cloudIds);

        Mockito.doReturn(cloudIds).when(uniqueIdentifierService).getCloudIdsByProvider("providerId", "recordId", 4);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/cloudIds")
                .param(UISParamConstants.Q_FROM, "recordId")
                .param(UISParamConstants.Q_LIMIT, String.valueOf(limit))
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ResultSlice<CloudId> result = new ObjectMapper().readValue(
                content, new TypeReference<ResultSlice<CloudId>>() {
                });

        assertThat(result.getResults().size(), is(2));
        CloudId firstEntryFromResultSlice = result.getResults().get(0);
        assertEquals(firstEntryFromResultSlice.getId(), cloudId.getId());
        assertEquals(firstEntryFromResultSlice.getLocalId().getProviderId(), cloudId
                .getLocalId().getProviderId());
        assertEquals(firstEntryFromResultSlice.getLocalId().getRecordId(), cloudId
                .getLocalId().getRecordId());
        assertThat(result.getNextSlice(), nullValue());
    }

    /**
     * Test the exception that a provider does not exist
     */
    @Test
    public void testGetLocalIdsByProviderProviderDoesNotExistException()
            throws Exception {
        Throwable exception = new ProviderDoesNotExistException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getHttpCode(),
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getErrorInfo("providerId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getLocalIdsByProvider("providerId",
                "recordId", 10000);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/localIds").param("from", "recordId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getDetails());
    }

    /**
     * The the retrieval of cloud ids based on a provider
     */
    @Test
    public void testGetCloudIdsByProvider()
            throws Exception {
        //given
        ResultSlice<CloudId> cloudIdListWrapper = new ResultSlice<>();
        List<CloudId> cloudIdList = new ArrayList<>();
        cloudIdList.add(createCloudId("providerId", "recordId"));
        cloudIdListWrapper.setResults(cloudIdList);

        Mockito.doReturn(cloudIdList).when(uniqueIdentifierService).getCloudIdsByProvider("providerId", "recordId", 10001);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/cloudIds").param("from", "recordId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();


        String content = mvcResult.getResponse().getContentAsString();
        ResultSlice<CloudId> retList = new ObjectMapper().readValue(
                content, new TypeReference<ResultSlice<CloudId>>() {
                });

        assertThat(retList.getResults().size(), is(cloudIdListWrapper.getResults().size()));
        assertEquals(retList.getResults().get(0).getId(), cloudIdListWrapper.getResults().get(0).getId());
        assertEquals(retList.getResults().get(0).getLocalId().getProviderId(), cloudIdListWrapper.getResults().get(0)
                .getLocalId().getProviderId());
        assertEquals(retList.getResults().get(0).getLocalId().getRecordId(), cloudIdListWrapper.getResults().get(0)
                .getLocalId().getRecordId());
    }

    /**
     * Test the database exception
     */
    @Test
    public void testGetCloudIdsByProviderDBException() throws Exception {
        Throwable exception = new DatabaseConnectionException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getHttpCode(),
                        IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
                                .getErrorInfo(
                                        uniqueIdentifierService.getHostList(),
                                        uniqueIdentifierService.getPort(), "")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getCloudIdsByProvider("providerId",
                "recordId", 10001);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/cloudIds").param("from", "recordId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();


        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

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

    /**
     * Test the exception of cloud ids when a provider does not exist
     */
    @Test
    public void testGetCloudIdsByProviderProviderDoesNotExistException()
            throws Exception {
        Throwable exception = new ProviderDoesNotExistException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getHttpCode(),
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getErrorInfo("providerId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getCloudIdsByProvider("providerId",
                "recordId", 10001);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/cloudIds").param("from", "recordId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();


        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getDetails());
    }

    /**
     * Test the retrieval of an empty dataset based on provider search
     */
    @Test
    public void testGetCloudIdsByProviderRecordDatasetEmptyException()
            throws Exception {
        Throwable exception = new RecordDatasetEmptyException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.RECORDSET_EMPTY.getHttpCode(),
                        IdentifierErrorTemplate.RECORDSET_EMPTY
                                .getErrorInfo("providerId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).getCloudIdsByProvider("providerId",
                "recordId", 10001);

        MvcResult mvcResult = mockMvc.perform(get("/data-providers/providerId/cloudIds").param("from", "recordId")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(
                        "providerId").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(
                        "providerId").getDetails());
    }

    /**
     * Test the creation of a mapping between a cloud id and a record id
     */
    @Test
    public void testCreateMapping() throws Exception {
        CloudId gid = createCloudId("providerId", "recordId");

        Mockito.doReturn(gid).when(uniqueIdentifierService).createCloudId("providerId", "recordId");
        Mockito.doReturn(gid).when(uniqueIdentifierService).createIdMapping(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(gid).when(uniqueIdentifierService).createIdMapping(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(post("/cloudIds")
                .param(UISParamConstants.Q_PROVIDER_ID, "providerId")
                .param(UISParamConstants.Q_RECORD_ID, "recordId")
                .accept(MediaType.APPLICATION_JSON));

        mockMvc.perform(post("/data-providers/providerId/cloudIds/" + gid.getId())
                .param(UISParamConstants.Q_RECORD_ID, "local1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }

    /**
     * Test the database exception
     */
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

        MvcResult mvcResult = mockMvc.perform(post("/data-providers/providerId/cloudIds/cloudId")
                .param(UISParamConstants.Q_RECORD_ID, "local1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);


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

    /**
     * Test the exception of a a missing cloud id between the mapping of a cloud
     * id and a record id
     */
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

        MvcResult mvcResult = mockMvc.perform(post("/data-providers/providerId/cloudIds/cloudId")
                .param(UISParamConstants.Q_RECORD_ID, "local1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(
                        "cloudId").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(
                        "cloudId").getDetails());
    }

    /**
     * Test the exception when a recordd id is mapped twice
     */
    @Test
    public void testCreateMappingIdHasBeenMapped() throws Exception {
        Throwable exception = new IdHasBeenMappedException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED
                                .getHttpCode(),
                        IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED
                                .getErrorInfo("local1", "providerId", "cloudId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).createIdMapping(
                "cloudId", "providerId", "local1");

        MvcResult mvcResult = mockMvc.perform(post("/data-providers/providerId/cloudIds/cloudId")
                .param(UISParamConstants.Q_RECORD_ID, "local1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(
                        "local1", "providerId", "cloudId").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(
                        "local1", "providerId", "cloudId").getDetails());
    }

    /**
     * Test the removal of a mapping between a cloud id and a record id
     */
    @Test
    public void testRemoveMapping() throws Exception {
        CloudId gid = createCloudId("providerId", "recordId");
        Mockito.reset(uniqueIdentifierService);
        Mockito.when(uniqueIdentifierService.createCloudId("providerId", "recordId")).thenReturn(gid);

        mockMvc.perform(get("/cloudIds")
                .param("providerId", "providerId")
                .param(UISParamConstants.Q_RECORD_ID, "recordId")
                .accept(MediaType.APPLICATION_JSON));

        mockMvc.perform(delete("/data-providers/providerId/localIds/recordId"))
                .andExpect(status().isOk());
    }

    /**
     * Test the database exception
     */
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

        MvcResult mvcResult = mockMvc.perform(delete("/data-providers/providerId/localIds/recordId").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().is5xxServerError()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new ObjectMapper().readValue(
                content, ErrorInfo.class);

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

    /**
     * Test the exception when the provider used for the removal of a mapping is
     * non existent
     */
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

        MvcResult mvcResult = mockMvc.perform(delete("/data-providers/providerId/localIds/recordId").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(errorInfo.getErrorCode(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getErrorCode());
        assertEquals(errorInfo.getDetails(),
                IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
                        "providerId").getDetails());
    }

    /**
     * Test the exception when a record to be removed does not exist
     */
    @Test
    public void testRemoveMappingRecordIdDoesNotExistException()
            throws Exception {
        Throwable exception = new RecordIdDoesNotExistException(
                new IdentifierErrorInfo(
                        IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST
                                .getHttpCode(),
                        IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST
                                .getErrorInfo("recordId")));

        Mockito.doThrow(exception).when(uniqueIdentifierService).removeIdMapping(
                "providerId", "recordId");

        MvcResult mvcResult = mockMvc.perform(delete("/data-providers/providerId/localIds/recordId").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound()).andReturn();

        String content = mvcResult.getResponse().getContentAsString();
        ErrorInfo errorInfo = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                content, ErrorInfo.class);

        assertEquals(
                errorInfo.getErrorCode(),
                IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(
                        "recordId").getErrorCode());
        assertEquals(
                errorInfo.getDetails(),
                IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(
                        "recordId").getDetails());
    }
}
