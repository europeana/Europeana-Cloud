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
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.rest.BasicUniqueIdResource;
import eu.europeana.cloud.service.uis.rest.CloudIdList;
import eu.europeana.cloud.service.uis.rest.LocalIdList;
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

	@Override
	public Application configure() {
		return new ResourceConfig().registerClasses(BasicUniqueIdResource.class).property("contextConfigLocation",
				"classpath:ecloud-uidservice-context-test.xml");
	}

	/**
	 * 
	 */
	@Before
	public void mockUp() {
		ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
		uniqueIdentifierService = applicationContext.getBean(UniqueIdentifierService.class);
		Mockito.reset(uniqueIdentifierService);
	}

	/**
	 * 
	 */
	@Test
	public void testCreateGlobalId() {

		CloudId originalGid = createGlobalId(providerId, recordId);
		when(uniqueIdentifierService.createGlobalId(providerId, recordId)).thenReturn(originalGid);
		// Create a single object test
		Response response = target("/uniqueId/createRecordId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(response.getStatus(), is(200));
		CloudId retrieveCreate = response.readEntity(CloudId.class);
		assertEquals(originalGid.getId(), retrieveCreate.getId());
		assertEquals(originalGid.getLocalId().getProviderId(), retrieveCreate.getLocalId().getProviderId());
		assertEquals(originalGid.getLocalId().getRecordId(), retrieveCreate.getLocalId().getRecordId());

	}

	@Test
	public void testCreateGlobalIdDbException() {
		Throwable databaseException = new DatabaseConnectionException();

		when(uniqueIdentifierService.createGlobalId(providerId, recordId)).thenThrow(databaseException);

		Response resp = target("/uniqueId/createRecordId").queryParam(providerId, providerId)
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

	@Test
	public void testCreateGlobalIdRecordExistsException() {
		Throwable exception = new RecordExistsException();

		when(uniqueIdentifierService.createGlobalId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/createRecordId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(409));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.RECORD_EXISTS.getErrorInfo(providerId, recordId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.RECORD_EXISTS.getErrorInfo(providerId, recordId)
				.getDetails());
	}

	/**
	 * 
	 */
	@Test
	public void testGetGlobalId() {
		CloudId originalGid = createGlobalId(providerId, recordId);
		when(uniqueIdentifierService.getGlobalId(providerId, recordId)).thenReturn(originalGid);
		// Retrieve the single object by provider and recordId
		Response response = target("/uniqueId/getGlobalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().get();

		assertThat(response.getStatus(), is(200));
		CloudId retrieveGet = response.readEntity(CloudId.class);
		assertEquals(originalGid.getId(), retrieveGet.getId());
		assertEquals(originalGid.getLocalId().getProviderId(), retrieveGet.getLocalId().getProviderId());
		assertEquals(originalGid.getLocalId().getRecordId(), retrieveGet.getLocalId().getRecordId());
	}

	@Test
	public void testGetGlobalIdDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getGlobalId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/getGlobalId").queryParam(providerId, providerId)
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

	@Test
	public void testGetGlobalIdRecordDoesNotExistException() {
		Throwable exception = new RecordDoesNotExistException();

		when(uniqueIdentifierService.getGlobalId(providerId, recordId)).thenThrow(exception);

		Response resp = target("/uniqueId/getGlobalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorInfo.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId).getDetails());
	}

	/**
	 * 
	 */
	@Test
	public void testGetLocalIds() {
		LocalIdList lidListWrapper = new LocalIdList();
		List<LocalId> localIdList = new ArrayList<>();
		localIdList.add(createLocalId(providerId, recordId));
		String globalId = createGlobalId(providerId, recordId).getId();
		lidListWrapper.setList(localIdList);
		when(uniqueIdentifierService.getLocalIdsByGlobalId(globalId)).thenReturn(localIdList);
		Response response = target("/uniqueId/getLocalIds").queryParam("globalId", globalId).request().get();
		assertThat(response.getStatus(), is(200));
		LocalIdList retList = response.readEntity(LocalIdList.class);
		assertThat(retList.getList().size(), is(lidListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getProviderId(), lidListWrapper.getList().get(0).getProviderId());
		assertEquals(retList.getList().get(0).getRecordId(), lidListWrapper.getList().get(0).getRecordId());
	}

	@Test
	public void testGetLocalIdsDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getLocalIdsByGlobalId("globalId")).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIds").queryParam("globalId", "globalId")
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

	@Test
	public void testGetLocalIdsGlobalIdDoesNotExistException() {
		Throwable exception = new GlobalIdDoesNotExistException();

		when(uniqueIdentifierService.getLocalIdsByGlobalId("globalId")).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIds").queryParam("globalId", "globalId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo("globalId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo("globalId")
				.getDetails());
	}

	/**
	 * 
	 */
	@Test
	public void testGetLocalIdsByProvider() {
		LocalIdList lidListWrapper = new LocalIdList();
		List<LocalId> localIdList = new ArrayList<>();
		localIdList.add(createLocalId(providerId, recordId));
		lidListWrapper.setList(localIdList);
		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenReturn(localIdList);
		Response response = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().get();
		assertThat(response.getStatus(), is(200));
		LocalIdList retList = response.readEntity(LocalIdList.class);
		assertThat(retList.getList().size(), is(lidListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getProviderId(), lidListWrapper.getList().get(0).getProviderId());
		assertEquals(retList.getList().get(0).getRecordId(), lidListWrapper.getList().get(0).getRecordId());
	}

	@Test
	public void testGetLocalIdsByProviderDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId)
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

	@Test
	public void testGetLocalIdsByProviderProviderDoesNotExistException() {
		Throwable exception = new ProviderDoesNotExistException();

		when(uniqueIdentifierService.getLocalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getLocalIdsByProvider").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * 
	 */
	@Test
	public void testGetGlobalIdsByProvider() {
		CloudIdList globalIdListWrapper = new CloudIdList();
		List<CloudId> globalIdList = new ArrayList<>();
		globalIdList.add(createGlobalId(providerId, recordId));
		globalIdListWrapper.setList(globalIdList);
		when(uniqueIdentifierService.getGlobalIdsByProvider(providerId, recordId, 10000)).thenReturn(globalIdList);
		Response response = target("/uniqueId/getGlobalIdsByProvider").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().get();
		assertThat(response.getStatus(), is(200));
		CloudIdList retList = response.readEntity(CloudIdList.class);
		assertThat(retList.getList().size(), is(globalIdListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getId(), globalIdListWrapper.getList().get(0).getId());
		assertEquals(retList.getList().get(0).getLocalId().getProviderId(), globalIdListWrapper.getList().get(0)
				.getLocalId().getProviderId());
		assertEquals(retList.getList().get(0).getLocalId().getRecordId(), globalIdListWrapper.getList().get(0)
				.getLocalId().getRecordId());
	}

	@Test
	public void testGetGlobalIdsByProviderDBException() {
		Throwable exception = new DatabaseConnectionException();

		when(uniqueIdentifierService.getGlobalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getGlobalIdsByProvider").queryParam(providerId, providerId)
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

	@Test
	public void testGetGlobalIdsByProviderProviderDoesNotExistException() {
		Throwable exception = new ProviderDoesNotExistException();

		when(uniqueIdentifierService.getGlobalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getGlobalIdsByProvider").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId).getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)
				.getDetails());
	}

	@Test
	public void testGetGlobalIdsByProviderRecordDatasetEmptyException() {
		Throwable exception = new RecordDatasetEmptyException();

		when(uniqueIdentifierService.getGlobalIdsByProvider(providerId, recordId, 10000)).thenThrow(exception);

		Response resp = target("/uniqueId/getGlobalIdsByProvider").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(), IdentifierErrorInfo.RECORDSET_EMPTY.getErrorInfo(providerId)
				.getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.RECORDSET_EMPTY.getErrorInfo(providerId)
				.getDetails());
	}

	/**
	 * 
	 */
	@Test
	public void testCreateMapping() {
		CloudId gid = createGlobalId(providerId, recordId);
		when(uniqueIdentifierService.createGlobalId(providerId, recordId)).thenReturn(gid);
		// Create a single object test
		target("/uniqueId/createRecordId").queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		Response res = target("/uniqueId/createMapping").queryParam("globalId", gid.getId())
				.queryParam(providerId, providerId + "1").queryParam(recordId, recordId + "1").request().get();
		assertThat(res.getStatus(), is(200));
	}

	@Test
	public void testCreateMappingDBException() {
		Throwable exception = new DatabaseConnectionException();

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("globalId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("globalId", "globalId")
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

	@Test
	public void testCreateMappingGlobalIdException() {
		Throwable exception = new GlobalIdDoesNotExistException();

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("globalId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("globalId", "globalId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo("globalId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo("globalId")
				.getDetails());
	}

	@Test
	public void testCreateMappingIdHasBeenMmapped() {
		Throwable exception = new IdHasBeenMappedException();

		doThrow(exception).when(uniqueIdentifierService).createIdMapping("globalId", providerId, recordId);

		Response resp = target("/uniqueId/createMapping").queryParam("globalId", "globalId")
				.queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		assertThat(resp.getStatus(), is(409));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, "globalId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(),
				IdentifierErrorInfo.ID_HAS_BEEN_MAPPED.getErrorInfo(recordId, providerId, "globalId").getDetails());
	}

	/**
	 * 
	 */
	@Test
	public void testRemoveMapping() {
		CloudId gid = createGlobalId(providerId, recordId);
		when(uniqueIdentifierService.createGlobalId(providerId, recordId)).thenReturn(gid);
		target("/uniqueId/createRecordId").queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		Response res = target("/uniqueId/removeMappingByLocalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().delete();
		assertThat(res.getStatus(), is(200));

	}

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

	@Test
	public void testDeleteCloudId() {
		CloudId gid = createGlobalId(providerId, recordId);
		when(uniqueIdentifierService.createGlobalId(providerId, recordId)).thenReturn(gid);
		target("/uniqueId/createRecordId").queryParam(providerId, providerId).queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).get();
		Response res = target("/uniqueId/deleteGlobalId").queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().delete();
		assertThat(res.getStatus(), is(200));
	}

	@Test
	public void testDeleteCloudIdDBException() {
		Throwable exception = new DatabaseConnectionException();

		doThrow(exception).when(uniqueIdentifierService).deleteGlobalId("globalId");

		Response resp = target("/uniqueId/deleteGlobalId").queryParam("globalId", "globalId")
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

	@Test
	public void testDeleteCloudIdGlobalIdDoesNotExistException() {
		Throwable exception = new GlobalIdDoesNotExistException();

		doThrow(exception).when(uniqueIdentifierService).deleteGlobalId("globalId");

		Response resp = target("/uniqueId/deleteGlobalId").queryParam("globalId", "globalId")
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).delete();
		assertThat(resp.getStatus(), is(404));
		ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
		StringUtils.equals(errorInfo.getErrorCode(),
				IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo("globalId").getErrorCode());
		StringUtils.equals(errorInfo.getDetails(), IdentifierErrorInfo.GLOBALID_DOES_NOT_EXIST.getErrorInfo("globalId")
				.getDetails());
	}

	private static LocalId createLocalId(String providerId, String recordId) {
		LocalId localId = new LocalId();
		localId.setProviderId(providerId);
		localId.setRecordId(recordId);
		return localId;
	}

	private static CloudId createGlobalId(String providerId, String recordId) {
		CloudId globalId = new CloudId();
		globalId.setLocalId(createLocalId(providerId, recordId));
		globalId.setId(Base36.encode("/" + providerId + "/" + recordId));
		return globalId;
	}
}
