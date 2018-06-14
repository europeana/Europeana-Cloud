package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.*;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.JerseyConfig;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * DataProviderResourceTest
 */
public class DataProviderResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private UniqueIdentifierService uniqueIdentifierService;

    private WebTarget dataProviderWebTarget;

    @Override
    public Application configure() {
	return new JerseyConfig().property("contextConfigLocation",
		"classpath:/uis-context-test.xml");
    }

    /**
     * Retrieve the spring enabled mockups from the application context
     */
    @Before
    public void mockUp() {
	ApplicationContext applicationContext = ApplicationContextUtils
		.getApplicationContext();
	dataProviderService = applicationContext
		.getBean(DataProviderService.class);
	uniqueIdentifierService = applicationContext
		.getBean(UniqueIdentifierService.class);
	Mockito.reset(dataProviderService);
	dataProviderWebTarget = target(DataProviderResource.class
		.getAnnotation(Path.class).value());
    }

    /**
     * Update a provider
     * 
     * @throws ProviderAlreadyExistsException
     * @throws ProviderDoesNotExistException
     * @throws MalformedURLException
     */
    @Test
    public void shouldUpdateProvider() throws ProviderAlreadyExistsException,
	    ProviderDoesNotExistException, MalformedURLException {
	// given certain provider data
	String providerName = "provident";

	DataProvider dp = new DataProvider();
	dp.setId(providerName);
	// when the provider is updated
	DataProviderProperties properties = new DataProviderProperties();
	properties.setOrganisationName("Organizacja");
	properties.setRemarks("Remarks");
	dp.setProperties(properties);
	Mockito.when(
		dataProviderService.updateProvider(providerName, properties))
		.thenReturn(dp);
	WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(
		ParamConstants.P_PROVIDER, providerName);
	Response putResponse = providentWebTarget.request().put(
		Entity.json(properties));
	assertEquals(Response.Status.NO_CONTENT.getStatusCode(),
		putResponse.getStatus());
	dp.setProperties(properties);
	Mockito.when(dataProviderService.getProvider(providerName)).thenReturn(
		dp);
	// then the inserted provider should be in service
	DataProvider retProvider = dataProviderService
		.getProvider(providerName);
	assertEquals(providerName, retProvider.getId());
	assertEquals(properties, retProvider.getProperties());
    }

    /**
     * Get provider Unit tests
     * 
     * @throws ProviderAlreadyExistsException
     * @throws ProviderDoesNotExistException
     */
    @Test
    public void shouldGetProvider() throws ProviderAlreadyExistsException,
	    ProviderDoesNotExistException {
	// given certain provider in service
	DataProviderProperties properties = new DataProviderProperties();
	properties.setOrganisationName("Organizacja");
	properties.setRemarks("Remarks");
	String providerName = "provident";

	DataProvider dp = new DataProvider();
	dp.setId(providerName);

	dp.setProperties(properties);
	Mockito.when(dataProviderService.getProvider(providerName)).thenReturn(
			dp);
	// dataProviderService.createProvider(providerName, properties);

	// when you get provider by rest api
	WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(
			ParamConstants.P_PROVIDER, providerName);
	Response getResponse = providentWebTarget.request().get();
	assertEquals(Response.Status.OK.getStatusCode(),
			getResponse.getStatus());
	DataProvider receivedDataProvider = getResponse
		.readEntity(DataProvider.class);

	// then received provider should be the same as inserted
	assertEquals(providerName, receivedDataProvider.getId());
	assertEquals(properties, receivedDataProvider.getProperties());
    }

    /**
     * Test Non Existing provider
     * 
     * @throws ProviderDoesNotExistException
     */
    @Test
    public void shouldReturn404OnNotExistingProvider()
	    throws ProviderDoesNotExistException {
	// given there is no provider in service
	Mockito.when(dataProviderService.getProvider("provident")).thenThrow(
		new ProviderDoesNotExistException(new IdentifierErrorInfo(
			IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				.getHttpCode(),
			IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				.getErrorInfo("provident"))));
	// when you get certain provider
	WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(
			ParamConstants.P_PROVIDER, "provident");
	Response getResponse = providentWebTarget.request().get();

	// then you should get error that such does not exist
	assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
		getResponse.getStatus());
	ErrorInfo deleteErrorInfo = getResponse.readEntity(ErrorInfo.class);
	assertEquals(IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
		.getErrorInfo("provident").getErrorCode(),
		deleteErrorInfo.getErrorCode());
    }

	@Test
	public void shouldDeleteProvider() throws Exception{
		WebTarget providerWebTarget = dataProviderWebTarget.resolveTemplate(
				ParamConstants.P_PROVIDER, "provident");
		Response deleteResponse = providerWebTarget.request().delete();
		
		assertEquals(deleteResponse.getStatus(), 200);
	}
	
    /**
     * Test the retrieval of local Ids by a provider
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testGetLocalIdsByProvider()
            throws Exception {
        ResultSlice<CloudId> lidListWrapper = new ResultSlice<>();
        List<CloudId> localIdList = new ArrayList<>();
        localIdList.add(createCloudId("providerId", "recordId"));
        lidListWrapper.setResults(localIdList);
        when(uniqueIdentifierService.getLocalIdsByProvider("providerId", "recordId", 10000)).thenReturn(localIdList);
        Response response = target("/data-providers/providerId/localIds").queryParam("from", "recordId").request()
                .get();
        assertThat(response.getStatus(), is(200));
        ResultSlice<CloudId> retList = response.readEntity(ResultSlice.class);
        assertThat(retList.getResults().size(), is(lidListWrapper.getResults().size()));
        assertEquals(retList.getResults().get(0).getLocalId().getProviderId(), lidListWrapper.getResults().get(0)
                .getLocalId().getProviderId());
        assertEquals(retList.getResults().get(0).getLocalId().getRecordId(), lidListWrapper.getResults().get(0)
                .getLocalId().getRecordId());
    }

    /**
     * Test the database exception
     * 
     * @throws Exception
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

	when(
		uniqueIdentifierService.getLocalIdsByProvider("providerId",
			"recordId", 10000)).thenThrow(exception);

	Response resp = target("/data-providers/providerId/localIds")
		.queryParam("from", "recordId").request().get();
	assertThat(resp.getStatus(), is(500));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
			uniqueIdentifierService.getHostList(),
			uniqueIdentifierService.getHostList(), "")
			.getErrorCode());
	StringUtils
		.equals(errorInfo.getDetails(),
			IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				.getErrorInfo(
					uniqueIdentifierService.getHostList(),
					uniqueIdentifierService.getHostList(),
					"").getDetails());
    }

    /**
     * Test the exception that a provider does not exist
     * 
     * @throws Exception
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

	when(
		uniqueIdentifierService.getLocalIdsByProvider("providerId",
			"recordId", 10000)).thenThrow(exception);

	Response resp = target("/data-providers/providerId/localIds")
		.queryParam("from", "recordId").request().get();
	assertThat(resp.getStatus(), is(404));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
			"providerId").getErrorCode());
	StringUtils.equals(
		errorInfo.getDetails(),
		IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
			"providerId").getDetails());
    }

    /**
     * The the retrieval of cloud ids based on a provider
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testGetCloudIdsByProvider()
            throws Exception {
        //given
        ResultSlice<CloudId> cloudIdListWrapper = new ResultSlice<>();
        List<CloudId> cloudIdList = new ArrayList<>();
        cloudIdList.add(createCloudId("providerId", "recordId"));
        cloudIdListWrapper.setResults(cloudIdList);
        when(uniqueIdentifierService.getCloudIdsByProvider("providerId", "recordId", 10001)).thenReturn(cloudIdList);
        //when
        Response response = target("/data-providers/providerId/cloudIds").queryParam("from", "recordId").request()
                .get();
        //then
        assertThat(response.getStatus(), is(200));
        ResultSlice<CloudId> retList = response.readEntity(ResultSlice.class);
        assertThat(retList.getResults().size(), is(cloudIdListWrapper.getResults().size()));
        assertEquals(retList.getResults().get(0).getId(), cloudIdListWrapper.getResults().get(0).getId());
        assertEquals(retList.getResults().get(0).getLocalId().getProviderId(), cloudIdListWrapper.getResults().get(0)
                .getLocalId().getProviderId());
        assertEquals(retList.getResults().get(0).getLocalId().getRecordId(), cloudIdListWrapper.getResults().get(0)
                .getLocalId().getRecordId());
    }

    /**
     * The the retrieval of cloud ids based on a provider with specified limit
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
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
        when(uniqueIdentifierService.getCloudIdsByProvider("providerId", "recordId", 2)).thenReturn(cloudIds);
        //when
        Response response = target("/data-providers/providerId/cloudIds").queryParam(UISParamConstants.Q_FROM, "recordId").queryParam(UISParamConstants.Q_LIMIT, limit).request()
                .get();
        //then
        assertThat(response.getStatus(), is(200));
        ResultSlice<CloudId> result = response.readEntity(ResultSlice.class);
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
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
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
        when(uniqueIdentifierService.getCloudIdsByProvider("providerId", "recordId", 4)).thenReturn(cloudIds);
        //when
        Response response = target("/data-providers/providerId/cloudIds").queryParam(UISParamConstants.Q_FROM, "recordId").queryParam(UISParamConstants.Q_LIMIT, limit).request()
                .get();
        //then
        assertThat(response.getStatus(), is(200));
        ResultSlice<CloudId> result = response.readEntity(ResultSlice.class);
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
     * Test the database exception
     * 
     * @throws Exception
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

	when(
		uniqueIdentifierService.getCloudIdsByProvider("providerId",
			"recordId", 10001)).thenThrow(exception);

	Response resp = target("/data-providers/providerId/cloudIds")
		.queryParam("from", "recordId").request().get();
	assertThat(resp.getStatus(), is(500));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
			uniqueIdentifierService.getHostList(),
			uniqueIdentifierService.getHostList(), "")
			.getErrorCode());
	StringUtils
		.equals(errorInfo.getDetails(),
			IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				.getErrorInfo(
					uniqueIdentifierService.getHostList(),
					uniqueIdentifierService.getHostList(),
					"").getDetails());
    }

    /**
     * Test the exception of cloud ids when a provider does not exist
     * 
     * @throws Exception
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

	when(
		uniqueIdentifierService.getCloudIdsByProvider("providerId",
			"recordId", 10001)).thenThrow(exception);

	Response resp = target("/data-providers/providerId/cloudIds")
		.queryParam("from", "recordId").request().get();
	assertThat(resp.getStatus(), is(404));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
			"providerId").getErrorCode());
	StringUtils.equals(
		errorInfo.getDetails(),
		IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
			"providerId").getDetails());
    }

    /**
     * Test the retrieval of an empty dataset based on provider search
     * 
     * @throws Exception
     */
    @Test
    public void testGetCloudIdsByProviderRecordDatasetEmptyException()
	    throws Exception {
	Throwable exception = new RecordDatasetEmptyException(
		new IdentifierErrorInfo(
			IdentifierErrorTemplate.RECORDSET_EMPTY.getHttpCode(),
			IdentifierErrorTemplate.RECORDSET_EMPTY
				.getErrorInfo("providerId")));

	when(
		uniqueIdentifierService.getCloudIdsByProvider("providerId",
			"recordId", 10001)).thenThrow(exception);

	Response resp = target("/data-providers/providerId/cloudIds")
		.queryParam("from", "recordId").request().get();
	assertThat(resp.getStatus(), is(404));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(
			"providerId").getErrorCode());
	StringUtils.equals(
		errorInfo.getDetails(),
		IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(
			"providerId").getDetails());
    }

    /**
     * Test the creation of a mapping between a cloud id and a record id
     * 
     * @throws Exception
     */
    @Test
    public void testCreateMapping() throws Exception {
	CloudId gid = createCloudId("providerId", "recordId");
	when(uniqueIdentifierService.createCloudId("providerId", "recordId"))
		.thenReturn(gid);
	when(uniqueIdentifierService.createIdMapping(Mockito.anyString(),Mockito.anyString())).thenReturn(gid);
	when(uniqueIdentifierService.createIdMapping(Mockito.anyString(),Mockito.anyString(),Mockito.anyString())).thenReturn(gid);
	// Create a single object test
	target("cloudIds")
		.queryParam(UISParamConstants.Q_PROVIDER_ID, "providerId")
		.queryParam(UISParamConstants.Q_RECORD_ID, "recordId")
		.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML)
		.post(null);
	Response res = target(
		"/data-providers/providerId/cloudIds/" + gid.getId())
		.queryParam(UISParamConstants.Q_RECORD_ID, "local1").request()
		.post(Entity.text(""));
	assertThat(res.getStatus(), is(200));
    }

    /**
     * Test the database exception
     * 
     * @throws Exception
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

	doThrow(exception).when(uniqueIdentifierService).createIdMapping(
		"cloudId", "providerId", "local1");

	Response resp = target("/data-providers/providerId/cloudIds/cloudId")
		.queryParam(UISParamConstants.Q_RECORD_ID, "local1").request()
		.post(Entity.text(""));
	assertThat(resp.getStatus(), is(500));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
			uniqueIdentifierService.getHostList(),
			uniqueIdentifierService.getHostList(), "")
			.getErrorCode());
	StringUtils
		.equals(errorInfo.getDetails(),
			IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				.getErrorInfo(
					uniqueIdentifierService.getHostList(),
					uniqueIdentifierService.getHostList(),
					"").getDetails());
    }

    /**
     * Test the exception of a a missing cloud id between the mapping of a cloud
     * id and a record id
     * 
     * @throws Exception
     */
    @Test
    public void testCreateMappingCloudIdException() throws Exception {
	Throwable exception = new CloudIdDoesNotExistException(
		new IdentifierErrorInfo(
			IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
				.getHttpCode(),
			IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
				.getErrorInfo("cloudId")));

	doThrow(exception).when(uniqueIdentifierService).createIdMapping(
		"cloudId", "providerId", "local1");

	Response resp = target("/data-providers/providerId/cloudIds/cloudId")
		.queryParam(UISParamConstants.Q_RECORD_ID, "local1").request()
		.post(Entity.text(""));
	assertThat(resp.getStatus(), is(404));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(
			"cloudId").getErrorCode());
	StringUtils.equals(
		errorInfo.getDetails(),
		IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(
			"cloudId").getDetails());
    }

    /**
     * Test the exception when a recordd id is mapped twice
     * 
     * @throws Exception
     */
    @Test
    public void testCreateMappingIdHasBeenMapped() throws Exception {
	Throwable exception = new IdHasBeenMappedException(
		new IdentifierErrorInfo(
			IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED
				.getHttpCode(),
			IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED
				.getErrorInfo("local1", "providerId", "cloudId")));

	doThrow(exception).when(uniqueIdentifierService).createIdMapping(
		"cloudId", "providerId", "local1");

	Response resp = target("/data-providers/providerId/cloudIds/cloudId")
		.queryParam(UISParamConstants.Q_RECORD_ID, "local1").request()
		.post(Entity.text(""));
	assertThat(resp.getStatus(), is(409));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(
			"local1", "providerId", "cloudId").getErrorCode());
	StringUtils.equals(
		errorInfo.getDetails(),
		IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(
			"local1", "providerId", "cloudId").getDetails());
    }

    /**
     * Test the removal of a mapping between a cloud id and a record id
     * 
     * @throws Exception
     */
    @Test
    public void testRemoveMapping() throws Exception {
	CloudId gid = createCloudId("providerId", "recordId");
	when(uniqueIdentifierService.createCloudId("providerId", "recordId"))
		.thenReturn(gid);
	target("/cloudIds").queryParam("providerId", "providerId")
		.queryParam(UISParamConstants.Q_RECORD_ID, "recordId")
		.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML)
		.get();
	Response resp = target("/data-providers/providerId/localIds/recordId")
		.request().delete();
	assertThat(resp.getStatus(), is(200));

    }

    /**
     * Test the database exception
     * 
     * @throws Exception
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

	doThrow(exception).when(uniqueIdentifierService).removeIdMapping(
		"providerId", "recordId");

	Response resp = target("/data-providers/providerId/localIds/recordId")
		.request().delete();
	assertThat(resp.getStatus(), is(500));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
			uniqueIdentifierService.getHostList(),
			uniqueIdentifierService.getHostList(), "")
			.getErrorCode());
	StringUtils
		.equals(errorInfo.getDetails(),
			IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				.getErrorInfo(
					uniqueIdentifierService.getHostList(),
					uniqueIdentifierService.getHostList(),
					"").getDetails());
    }

    /**
     * Test the exception when the provider used for the removal of a mapping is
     * non existent
     * 
     * @throws Exception
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

	doThrow(exception).when(uniqueIdentifierService).removeIdMapping(
		"providerId", "recordId");

	Response resp = target("/data-providers/providerId/localIds/recordId")
		.request().delete();
	assertThat(resp.getStatus(), is(404));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
			"providerId").getErrorCode());
	StringUtils.equals(
		errorInfo.getDetails(),
		IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(
			"providerId").getDetails());
    }

    /**
     * Test the exception when a record to be removed does not exist
     * 
     * @throws Exception
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

	doThrow(exception).when(uniqueIdentifierService).removeIdMapping(
		"providerId", "recordId");

	Response resp = target("/data-providers/providerId/localIds/recordId")
		.request().delete();
	assertThat(resp.getStatus(), is(404));
	ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
	StringUtils.equals(
		errorInfo.getErrorCode(),
		IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(
			"recordId").getErrorCode());
	StringUtils.equals(
		errorInfo.getDetails(),
		IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(
			"recordId").getDetails());
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
