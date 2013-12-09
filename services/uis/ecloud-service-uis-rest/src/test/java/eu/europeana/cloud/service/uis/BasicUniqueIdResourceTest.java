package eu.europeana.cloud.service.uis;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistExceptionMapper;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionExceptionMapper;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedExceptionMapper;
import eu.europeana.cloud.service.uis.exception.ProviderDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.ProviderDoesNotExistExceptionMapper;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyExceptionMapper;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistExceptionMapper;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordExistsExceptionMapper;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistExceptionMapper;
import eu.europeana.cloud.service.uis.rest.BasicUniqueIdResource;
import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

/**
 * UniqueIdResource unit test
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 23, 2013
 */
public class BasicUniqueIdResourceTest extends JerseyTest {

	private UniqueIdentifierService uniqueIdentifierService;
	private String providerId = "providerId";
	private String recordId = "recordId";

	/**
	 * Configuration of the Spring context
	 */
	@Override
	public Application configure() {
		return new ResourceConfig().registerClasses(CloudIdDoesNotExistExceptionMapper.class)
				.registerClasses(DatabaseConnectionExceptionMapper.class)
				.registerClasses(IdHasBeenMappedExceptionMapper.class)
				.registerClasses(ProviderDoesNotExistExceptionMapper.class)
				.registerClasses(RecordDatasetEmptyExceptionMapper.class)
				.registerClasses(RecordDoesNotExistExceptionMapper.class)
				.registerClasses(RecordExistsExceptionMapper.class)
				.registerClasses(RecordIdDoesNotExistExceptionMapper.class)
				.registerClasses(BasicUniqueIdResource.class).property("contextConfigLocation",
				"classpath:ecloud-uidservice-context-test.xml");
	}

	/**
	 * Initialization of the Unique Identifier service mockup
	 */
	@Before
	public void mockUp() {
		ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
		uniqueIdentifierService = applicationContext.getBean(UniqueIdentifierService.class);
		Mockito.reset(uniqueIdentifierService);
	}

	/**
	 * Test to create a cloud Id
	 */
	@Test
	public void testCreateCloudId() throws Exception{

		CloudId originalGid = createCloudId(providerId, recordId);
		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(originalGid);
		// Create a single object test
		Response response = target("/uniqueId/createCloudIdLocal").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(response.getStatus(), is(200));
		CloudId retrieveCreate = response.readEntity(CloudId.class);
		assertEquals(originalGid.getId(), retrieveCreate.getId());
		assertEquals(originalGid.getLocalId().getProviderId(), retrieveCreate.getLocalId().getProviderId());
		assertEquals(originalGid.getLocalId().getRecordId(), retrieveCreate.getLocalId().getRecordId());

	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testCreateCloudIdDbException() throws Exception{
		Throwable databaseException = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenThrow(databaseException);

		Response resp = target("/uniqueId/createCloudIdLocal").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the a cloud id already exists for the record id
	 */
	@Test
	public void testCreateCloudIdRecordExistsException() throws Exception{
		Throwable exception = new RecordExistsException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.RECORD_EXISTS.getHttpCode(),
				IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId)));

		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/createCloudIdLocal").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(409));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId).getDetails());
	}

	/**
	 * Test the retrieval of a cloud id
	 */
	@Test
	public void testGetCloudId() throws Exception{
		CloudId originalGid = createCloudId(providerId, recordId);
		when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenReturn(originalGid);
		// Retrieve the single object by provider and recordId
		Response response = target("/uniqueId/getCloudId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().get();

		assertThat(response.getStatus(), is(200));
		CloudId retrieveGet = response.readEntity(CloudId.class);
		assertEquals(originalGid.getId(), retrieveGet.getId());
		assertEquals(originalGid.getLocalId().getProviderId(), retrieveGet.getLocalId().getProviderId());
		assertEquals(originalGid.getLocalId().getRecordId(), retrieveGet.getLocalId().getRecordId());
	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testGetCloudIdDBException() throws Exception{
		Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception that a gloabl id does not exist for this record id
	 */
	@Test
	public void testGetCloudIdRecordDoesNotExistException() throws Exception{
		Throwable exception = new RecordDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId)));

		when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getDetails());
	}

	/**
	 * Test the retrieval of local ids by a cloud id
	 */
	@Test
	public void testGetLocalIds() throws Exception{
		LocalIdList lidListWrapper = new LocalIdList();
		List<LocalId> localIdList = new ArrayList<>();
		localIdList.add(createLocalId(providerId, recordId));
		String cloudId = createCloudId(providerId, recordId).getId();
		lidListWrapper.setList(localIdList);
		when(uniqueIdentifierService.getLocalIdsByCloudId(cloudId)).thenReturn(localIdList);
		Response response = target("/uniqueId/getLocalIds").queryParam("cloudId", cloudId).request().get();
		assertThat(response.getStatus(), is(200));
		LocalIdList retList = response.readEntity(LocalIdList.class);
		assertThat(retList.getList().size(), is(lidListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getProviderId(), lidListWrapper.getList().get(0).getProviderId());
		assertEquals(retList.getList().get(0).getRecordId(), lidListWrapper.getList().get(0).getRecordId());
	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testGetLocalIdsDBException() throws Exception {
		Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		when(uniqueIdentifierService.getLocalIdsByCloudId("cloudId")).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIds").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test that a cloud id does not exist
	 */
	@Test
	public void testGetLocalIdsCloudIdDoesNotExistException() throws Exception {
		Throwable exception = new CloudIdDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")));

		when(uniqueIdentifierService.getLocalIdsByCloudId("cloudId")).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIds").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getDetails());
	}

	/**
	 * Test the retrieval of local Ids by a provider
	 */
	@Test
	public void testGetLocalIdsByProvider()throws Exception {
		LocalIdList lidListWrapper = new LocalIdList();
		List<LocalId> localIdList = new ArrayList<>();
		localIdList.add(createLocalId(providerId, recordId));
		lidListWrapper.setList(localIdList);
		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenReturn(localIdList);
		Response response = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request().get();
		assertThat(response.getStatus(), is(200));
		LocalIdList retList = response.readEntity(LocalIdList.class);
		assertThat(retList.getList().size(), is(lidListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getProviderId(), lidListWrapper.getList().get(0).getProviderId());
		assertEquals(retList.getList().get(0).getRecordId(), lidListWrapper.getList().get(0).getRecordId());
	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testGetLocalIdsByProviderDBException() throws Exception{
		Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception that a provider does not exist
	 */
	@Test
	public void testGetLocalIdsByProviderProviderDoesNotExistException() throws Exception{
		Throwable exception = new ProviderDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));

		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getDetails());
	}

	/**
	 * The the retrieval of cloud ids based on a provider
	 */
	@Test
	public void testGetCloudIdsByProvider() throws Exception{
		CloudIdList cloudIdListWrapper = new CloudIdList();
		List<CloudId> cloudIdList = new ArrayList<>();
		cloudIdList.add(createCloudId(providerId, recordId));
		cloudIdListWrapper.setList(cloudIdList);
		when(uniqueIdentifierService.getCloudIdsByProvider(providerId, recordId, 10000)).thenReturn(cloudIdList);
		Response response = target("/uniqueId/getCloudIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request().get();
		assertThat(response.getStatus(), is(200));
		CloudIdList retList = response.readEntity(CloudIdList.class);
		assertThat(retList.getList().size(), is(cloudIdListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getId(), cloudIdListWrapper.getList().get(0).getId());
		assertEquals(retList.getList().get(0).getLocalId().getProviderId(), cloudIdListWrapper.getList().get(0)
				.getLocalId().getProviderId());
		assertEquals(retList.getList().get(0).getLocalId().getRecordId(), cloudIdListWrapper.getList().get(0)
				.getLocalId().getRecordId());
	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testGetCloudIdsByProviderDBException() throws Exception{
		Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		when(uniqueIdentifierService.getCloudIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception of cloud ids when a provider does not exist
	 */
	@Test
	public void testGetCloudIdsByProviderProviderDoesNotExistException() throws Exception{
		Throwable exception = new ProviderDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));

		when(uniqueIdentifierService.getCloudIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getDetails());
	}

	/**
	 * Test the retrieval of an empty dataset based on provider search
	 */
	@Test
	public void testGetCloudIdsByProviderRecordDatasetEmptyException() throws Exception{
		Throwable exception = new RecordDatasetEmptyException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.RECORDSET_EMPTY.getHttpCode(),
				IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(providerId)));

		when(uniqueIdentifierService.getCloudIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(), IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(providerId)
				.getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * Test the creation of a mapping between a cloud id and a record id
	 */
	@Test
	public void testCreateMapping() throws Exception{
		CloudId gid = createCloudId(providerId, recordId);
		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(gid);
		// Create a single object test
		target("/uniqueId/createRecordId").queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		Response res = target("/uniqueId/createMapping").queryParam("cloudId", gid.getId())
				.queryParam(providerId, providerId + "1").queryParam(recordId, recordId + "1").request().get();
		assertThat(res.getStatus(), is(200));
	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testCreateMappingDBException() throws Exception{
		Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("cloudId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("cloudId", "cloudId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception of a a missing cloud id between the mapping of a cloud
	 * id and a record id
	 */
	@Test
	public void testCreateMappingCloudIdException() throws Exception{
		Throwable exception = new CloudIdDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")));

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("cloudId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("cloudId", "cloudId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getDetails());
	}

	/**
	 * Test the exception when a recordd id is mapped twice
	 */
	@Test
	public void testCreateMappingIdHasBeenMmapped() throws Exception{
		Throwable exception = new IdHasBeenMappedException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getHttpCode(),
				IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, "cloudId")));

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("cloudId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("cloudId", "cloudId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(409));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils
				.equals(errorInfo.getErrorCode(),
						IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, "cloudId")
								.getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, "cloudId").getDetails());
	}

	/**
	 * Test the removal of a mapping between a cloud id and a record id
	 */
	@Test
	public void testRemoveMapping() throws Exception{
		CloudId gid = createCloudId(providerId, recordId);
		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(gid);
		target("/uniqueId/createRecordId").queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		Response res = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().delete();
		assertThat(res.getStatus(), is(200));

	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testRemoveMappingDBException() throws Exception{
		Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		doThrow(exception).when(uniqueIdentifierService).removeIdMapping(providerId, recordId);

		Response resp = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception when the provider used for the removal of a mapping is
	 * non existent
	 */
	@Test
	public void testRemoveMappingProviderDoesNotExistException()throws Exception {
		Throwable exception = new ProviderDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));

		doThrow(exception).when(uniqueIdentifierService).removeIdMapping(providerId, recordId);

		Response resp = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getDetails());
	}

	/**
	 * Test the exception when a record to be removed does not exist
	 */
	@Test
	public void testRemoveMappingRecordIdDoesNotExistException()throws Exception {
		Throwable exception = new RecordIdDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(recordId)));

		doThrow(exception).when(uniqueIdentifierService).removeIdMapping(providerId, recordId);

		Response resp = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(recordId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(recordId).getDetails());
	}

	/**
	 * Test the deletion of a cloud id
	 */
	@Test
	public void testDeleteCloudId() throws Exception{
		CloudId gid = createCloudId(providerId, recordId);
		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenReturn(gid);
		target("/uniqueId/createRecordId").queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		Response res = target("/uniqueId/deleteCloudId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().delete();
		assertThat(res.getStatus(), is(200));
	}

	/**
	 * Test the database exception
	 */
	@Test
	public void testDeleteCloudIdDBException() throws Exception{
		Throwable exception = new DatabaseConnectionException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getPort(), "")));

		doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

		Response resp = target("/uniqueId/deleteCloudId").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception of the removal of a cloud id when a cloud id does not
	 * exist
	 */
	@Test
	public void testDeleteCloudIdCloudIdDoesNotExistException() throws Exception{
		Throwable exception = new CloudIdDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")));

		doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

		Response resp = target("/uniqueId/deleteCloudId").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
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
		cloudId.setId(Base36.encode("/" + providerId + "/" + recordId));
		return cloudId;
	}
}
