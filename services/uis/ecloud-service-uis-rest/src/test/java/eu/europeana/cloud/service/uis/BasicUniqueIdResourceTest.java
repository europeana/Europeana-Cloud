package eu.europeana.cloud.service.uis;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

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
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.CloudIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.rest.BasicUniqueIdResource;
import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

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
		return new ResourceConfig().registerClasses(BasicUniqueIdResource.class).property("contextConfigLocation",
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
	public void testCreateCloudId() {

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
	public void testCreateCloudIdDbException() {
		Throwable databaseException = new DatabaseConnectionException();

		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenThrow(databaseException);

		Response resp = target("/uniqueId/createCloudIdLocal").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the a cloud id already exists for the record id
	 */
	@Test
	public void testCreateCloudIdRecordExistsException() {
		Throwable exception = new RecordExistsException();

		when(uniqueIdentifierService.createCloudId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/createCloudIdLocal").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(409));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.RECORD_EXISTS.getErrorInfo(providerId, recordId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.RECORD_EXISTS.getErrorInfo(providerId, recordId)
				.getDetails());
	}

	/**
	 * Test the retrieval of a cloud id
	 */
	@Test
	public void testGetCloudId() {
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
	public void testGetCloudIdDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception that a gloabl id does not exist for this record id
	 */
	@Test
	public void testGetCloudIdRecordDoesNotExistException() {
		Throwable exception = new RecordDoesNotExistException();

		when(uniqueIdentifierService.getCloudId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorInfo.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getDetails());
	}

	/**
	 * Test the retrieval of local ids by a cloud id
	 */
	@Test
	public void testGetLocalIds() {
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
	public void testGetLocalIdsDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getLocalIdsByCloudId("cloudId")).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIds").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test that a cloud id does not exist
	 */
	@Test
	public void testGetLocalIdsCloudIdDoesNotExistException() {
		Throwable exception = new CloudIdDoesNotExistException();

		when(uniqueIdentifierService.getLocalIdsByCloudId("cloudId")).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIds").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")
				.getDetails());
	}

	/**
	 * Test the retrieval of local Ids by a provider
	 */
	@Test
	public void testGetLocalIdsByProvider() {
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
	public void testGetLocalIdsByProviderDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId).queryParam("start",recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception that a provider does not exist
	 */
	@Test
	public void testGetLocalIdsByProviderProviderDoesNotExistException() {
		Throwable exception = new ProviderDoesNotExistException();

		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * The the retrieval of cloud ids based on a provider
	 */
	@Test
	public void testGetCloudIdsByProvider() {
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
	public void testGetCloudIdsByProviderDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getCloudIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception of cloud ids when a provider does not exist
	 */
	@Test
	public void testGetCloudIdsByProviderProviderDoesNotExistException() {
		Throwable exception = new ProviderDoesNotExistException();

		when(uniqueIdentifierService.getCloudIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * Test the retrieval of an empty dataset based on provider search
	 */
	@Test
	public void testGetCloudIdsByProviderRecordDatasetEmptyException() {
		Throwable exception = new RecordDatasetEmptyException();

		when(uniqueIdentifierService.getCloudIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getCloudIdsByProvider").queryParam(providerId, providerId)
				.queryParam("start", recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(), IdentifierErrorInfo.RECORDSET_EMPTY.getErrorInfo(providerId)
				.getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.RECORDSET_EMPTY.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * Test the creation of a mapping between a cloud id and a record id
	 */
	@Test
	public void testCreateMapping() {
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
	public void testCreateMappingDBException() {
		Throwable exception = new DatabaseConnectionException();

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("cloudId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("cloudId", "cloudId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception of a a missing cloud id between the mapping of a cloud id and a record id
	 */
	@Test
	public void testCreateMappingCloudIdException() {
		Throwable exception = new CloudIdDoesNotExistException();

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("cloudId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("cloudId", "cloudId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")
				.getDetails());
	}

	/**
	 * Test the exception when a recordd id is mapped twice
	 */
	@Test
	public void testCreateMappingIdHasBeenMmapped() {
		Throwable exception = new IdHasBeenMappedException();

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("cloudId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("cloudId", "cloudId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(409));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, "cloudId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, "cloudId").getDetails());
	}

	/**
	 * Test the removal of a mapping between a cloud id and a record id
	 */
	@Test
	public void testRemoveMapping() {
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
	public void testRemoveMappingDBException() {
		Throwable exception = new DatabaseConnectionException();

		doThrow(exception).when(uniqueIdentifierService).removeIdMapping(providerId, recordId);

		Response resp = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception when the provider used for the removal of a mapping is non existent
	 */
	@Test
	public void testRemoveMappingProviderDoesNotExistException() {
		Throwable exception = new ProviderDoesNotExistException();

		doThrow(exception).when(uniqueIdentifierService).removeIdMapping(providerId, recordId);

		Response resp = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * Test the exception when a record to be removed does not exist
	 */
	@Test
	public void testRemoveMappingRecordIdDoesNotExistException() {
		Throwable exception = new ProviderDoesNotExistException();

		doThrow(exception).when(uniqueIdentifierService).removeIdMapping(providerId, recordId);

		Response resp = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * Test the deletion of a cloud id
	 */
	@Test
	public void testDeleteCloudId() {
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
	public void testDeleteCloudIdDBException() {
		Throwable exception = new DatabaseConnectionException();

		doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

		Response resp = target("/uniqueId/deleteCloudId").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(500));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(
				errorInfo.getErrorCode(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getErrorCode());
		StringUtils.equals(
				errorInfo.getDetails(),
				IdentifierErrorInfo.DATABASE_CONNECTION_ERROR.getErrorInfo(uniqueIdentifierService.getHost(),
						uniqueIdentifierService.getHost(), "").getDetails());
	}

	/**
	 * Test the exception of the removal of a cloud id when a cloud id does not exist
	 */
	@Test
	public void testDeleteCloudIdCloudIdDoesNotExistException() {
		Throwable exception = new CloudIdDoesNotExistException();

		doThrow(exception).when(uniqueIdentifierService).deleteCloudId("cloudId");

		Response resp = target("/uniqueId/deleteCloudId").queryParam("cloudId", "cloudId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.CLOUDID_DOES_NOT_EXIST.getErrorInfo("cloudId")
				.getDetails());
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
