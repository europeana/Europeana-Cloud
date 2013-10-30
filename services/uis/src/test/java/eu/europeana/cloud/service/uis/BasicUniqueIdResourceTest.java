package eu.europeana.cloud.service.uis;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.GlobalId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.rest.BasicUniqueIdResource;
import eu.europeana.cloud.service.uis.rest.GlobalIdList;
import eu.europeana.cloud.service.uis.rest.LocalIdList;

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
		return new ResourceConfig()
				.registerClasses(BasicUniqueIdResource.class).property(
						"contextConfigLocation",
						"classpath:ecloud-uidservice-context-test.xml");
	}

	/**
	 * 
	 */
	@Before
	public void mockUp() {
		ApplicationContext applicationContext = ApplicationContextUtils
				.getApplicationContext();
		uniqueIdentifierService = applicationContext
				.getBean(UniqueIdentifierService.class);
		Mockito.reset(uniqueIdentifierService);
	}

	/**
	 * 
	 */
	@Test
	public void testCreateGlobalId() {

		GlobalId originalGid = createGlobalId(providerId, recordId);
		when(uniqueIdentifierService.createGlobalId(providerId, recordId))
				.thenReturn(originalGid);
		// Create a single object test
		Response response = target("/uniqueId/createRecordId")
				.queryParam(providerId, providerId)
				.queryParam(recordId, recordId)
				.request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML)
				.get();
		assertThat(response.getStatus(), is(200));
		GlobalId retrieveCreate = response.readEntity(GlobalId.class);
		assertEquals(originalGid.getId(), retrieveCreate.getId());
		assertEquals(originalGid.getLocalId().getProviderId(), retrieveCreate
				.getLocalId().getProviderId());
		assertEquals(originalGid.getLocalId().getRecordId(), retrieveCreate
				.getLocalId().getRecordId());

	}

	/**
	 * 
	 */
	@Test
	public void testGetGlobalId() {
		GlobalId originalGid = createGlobalId(providerId, recordId);
		when(uniqueIdentifierService.getGlobalId(providerId, recordId))
				.thenReturn(originalGid);
		// Retrieve the single object by provider and recordId
		Response response = target("/uniqueId/getGlobalId")
				.queryParam(providerId, providerId)
				.queryParam(recordId, recordId).request().get();

		assertThat(response.getStatus(), is(200));
		GlobalId retrieveGet = response.readEntity(GlobalId.class);
		assertEquals(originalGid.getId(), retrieveGet.getId());
		assertEquals(originalGid.getLocalId().getProviderId(), retrieveGet
				.getLocalId().getProviderId());
		assertEquals(originalGid.getLocalId().getRecordId(), retrieveGet
				.getLocalId().getRecordId());
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
		when(uniqueIdentifierService.getLocalIdsByGlobalId(globalId))
				.thenReturn(localIdList);
		Response response = target("/uniqueId/getLocalIds")
				.queryParam("globalId", globalId).request().get();
		assertThat(response.getStatus(), is(200));
		LocalIdList retList = response.readEntity(LocalIdList.class);
		assertThat(retList.getList().size(),
				is(lidListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getProviderId(), lidListWrapper
				.getList().get(0).getProviderId());
		assertEquals(retList.getList().get(0).getRecordId(), lidListWrapper
				.getList().get(0).getRecordId());
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
		when(
				uniqueIdentifierService.getLocalIdsByProvider(providerId, 0,
						10000)).thenReturn(localIdList);
		Response response = target("/uniqueId/getLocalIdsByProvider")
				.queryParam(providerId, providerId).request().get();
		assertThat(response.getStatus(), is(200));
		LocalIdList retList = response.readEntity(LocalIdList.class);
		assertThat(retList.getList().size(),
				is(lidListWrapper.getList().size()));
		assertEquals(retList.getList().get(0).getProviderId(), lidListWrapper
				.getList().get(0).getProviderId());
		assertEquals(retList.getList().get(0).getRecordId(), lidListWrapper
				.getList().get(0).getRecordId());
	}

	/**
	 * 
	 */
	@Test
	public void testGetGlobalIdsByProvider() {
		GlobalIdList globalIdListWrapper = new GlobalIdList();
		List<GlobalId> globalIdList = new ArrayList<>();
		globalIdList.add(createGlobalId(providerId, recordId));
		globalIdListWrapper.setList(globalIdList);
		when(
				uniqueIdentifierService.getGlobalIdsByProvider(providerId, 0,
						10000)).thenReturn(globalIdList);
		Response response = target("/uniqueId/getGlobalIdsByProvider")
				.queryParam(providerId, providerId).request().get();
		assertThat(response.getStatus(), is(200));
		GlobalIdList retList = response.readEntity(GlobalIdList.class);
		assertThat(retList.getList().size(), is(globalIdListWrapper.getList()
				.size()));
		assertEquals(retList.getList().get(0).getId(), globalIdListWrapper
				.getList().get(0).getId());
		assertEquals(retList.getList().get(0).getLocalId().getProviderId(),
				globalIdListWrapper.getList().get(0).getLocalId()
						.getProviderId());
		assertEquals(retList.getList().get(0).getLocalId().getRecordId(),
				globalIdListWrapper.getList().get(0).getLocalId().getRecordId());
	}

	/**
	 * 
	 */
	@Test
	public void testCreateIdMapping() {
		GlobalId gid = createGlobalId(providerId, recordId);
		Response res = target("/uniqueId/createIdMapping")
				.queryParam(gid.getId(), providerId + "1", recordId + "1")
				.request().get();
		assertEquals(res.getStatusInfo().getStatusCode(), Status.OK);
		assertNotNull(gid);
	}

	private static LocalId createLocalId(String providerId, String recordId) {
		LocalId localId = new LocalId();
		localId.setProviderId(providerId);
		localId.setRecordId(recordId);
		return localId;
	}

	private static GlobalId createGlobalId(String providerId, String recordId) {
		GlobalId globalId = new GlobalId();
		globalId.setLocalId(createLocalId(providerId, recordId));
		globalId.setId("/" + providerId + "/" + recordId);
		return globalId;
	}
}
