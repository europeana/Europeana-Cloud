package eu.europeana.cloud.client.uis.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.exception.GenericException;

public class UISClientTest {
	
	UISClient client;
	
	@Before
	public void prepare(){
		client = mock(UISClient.class);
	}
	/**
	 * Test cloud id creation
	 */
	@Test
	public void testCreateCloudId(){
		try {
			CloudId actual = createCloudId();
			when(client.createCloudId("test", "test")).thenReturn(actual);
			CloudId expected = client.createCloudId("test", "test");
			assertEquals(expected,actual);
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test cloud id exception
	 */
	@Test
	public void testCreateCloudIdException(){
		try {
			when(client.createCloudId("test", "test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.createCloudId("test", "test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test cloud id retrieval
	 */
	@Test
	public void testGetCloudId(){
		try {
			CloudId actual = createCloudId();
			when(client.getCloudId("test", "test")).thenReturn(actual);
			CloudId expected = client.getCloudId("test", "test");
			assertEquals(expected,actual);
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test cloud id exception
	 */
	@Test
	public void testGetCloudIdException(){
		try {
			when(client.getCloudId("test", "test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.getCloudId("test", "test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test local ids retrieval
	 */
	@Test
	public void testGetRecordIds(){
		try {
			CloudId actual = createCloudId();
			List<CloudId> idList = new ArrayList<>();
			idList.add(actual);
			when(client.getRecordId(actual.getId())).thenReturn(idList);
			List<CloudId> expected = client.getRecordId(actual.getId());
			assertEquals(expected.size(),idList.size());
			assertEquals(idList.get(0), actual);
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test local id exception
	 */
	@Test
	public void testGetRecordIdException(){
		try {
			when(client.getRecordId("test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.getRecordId("test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	
	/**
	 * Test cloud ids retrieval
	 */
	@Test
	public void testGetCloudIds(){
		try {
			CloudId actual = createCloudId();
			List<CloudId> idList = new ArrayList<>();
			idList.add(actual);
			when(client.getCloudIdsByProvider("test")).thenReturn(idList);
			List<CloudId> expected = client.getCloudIdsByProvider("test");
			assertEquals(expected.size(),idList.size());
			assertEquals(idList.get(0), actual);
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test cloud ids exception
	 */
	@Test
	public void testGetCloudIdsException(){
		try {
			when(client.getCloudIdsByProvider("test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.getCloudIdsByProvider("test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test local ids by provider retrieval
	 */
	@Test
	public void testGetRecordIdsByProvider(){
		try {
			LocalId actual = createLocalId();
			List<LocalId> idList = new ArrayList<>();
			idList.add(actual);
			when(client.getRecordIdsByProvider("test")).thenReturn(idList);
			List<LocalId> expected = client.getRecordIdsByProvider("test");
			assertEquals(expected.size(),idList.size());
			assertEquals(idList.get(0), actual);
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test local id exception
	 */
	@Test
	public void testGetRecordIdsByProviderException(){
		try {
			when(client.getRecordIdsByProvider("test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.getRecordIdsByProvider("test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test local ids by provider with pagination retrieval
	 */
	@Test
	public void testGetRecordIdsByProviderWithPagination(){
		try {
			LocalId actual = createLocalId();
			List<LocalId> idList = new ArrayList<>();
			idList.add(actual);
			when(client.getRecordIdsByProviderWithPagination("test","test",1)).thenReturn(idList);
			List<LocalId> expected = client.getRecordIdsByProviderWithPagination("test","test",1);
			assertEquals(expected.size(),idList.size());
			assertEquals(idList.get(0), actual);
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test local ids by provider with pagination retrieval exception
	 */
	@Test
	public void testGetRecordIdsByProviderExceptionWithPagination(){
		try {
			when(client.getRecordIdsByProviderWithPagination("test","test",1)).thenThrow(new CloudException("test", new GenericException("test")));
			client.getRecordIdsByProviderWithPagination("test","test",1);	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test cloud ids by provider with pagination retrieval
	 */
	@Test
	public void testGetCloudIdsByProviderWithPagination(){
		try {
			CloudId actual = createCloudId();
			List<CloudId> idList = new ArrayList<>();
			idList.add(actual);
			when(client.getCloudIdsByProviderWithPagination("test","test",1)).thenReturn(idList);
			List<CloudId> expected = client.getCloudIdsByProviderWithPagination("test","test",1);
			assertEquals(expected.size(),idList.size());
			assertEquals(idList.get(0), actual);
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test cloud ids by provider with pagination retrieval exception
	 */
	@Test
	public void testGetCloudIdsByProviderExceptionWithPagination(){
		try {
			when(client.getCloudIdsByProviderWithPagination("test","test",1)).thenThrow(new CloudException("test", new GenericException("test")));
			client.getCloudIdsByProviderWithPagination("test","test",1);	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test create mapping
	 */
	@Test
	public void testCreateMapping(){
		try {
			when(client.createMapping("test","test","test")).thenReturn(true);
			assertTrue(client.createMapping("test","test","test"));
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test create mapping exception
	 */
	@Test
	public void testCreateMappingException(){
		try {
			when(client.createMapping("test","test","test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.createMapping("test","test","test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test remove mapping
	 */
	@Test
	public void testRemoveMapping(){
		try {
			when(client.removeMappingByLocalId("test","test")).thenReturn(true);
			assertTrue(client.removeMappingByLocalId("test","test"));
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test remove mapping exception
	 */
	@Test
	public void testRemoveMappingException(){
		try {
			when(client.removeMappingByLocalId("test","test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.removeMappingByLocalId("test","test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	/**
	 * Test delete cloud id
	 */
	@Test
	public void testDeleteCloudId(){
		try {
			when(client.deleteCloudId("test")).thenReturn(true);
			assertTrue(client.deleteCloudId("test"));
		} catch (CloudException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Test delete cloud id exception
	 */
	@Test
	public void testDeleteCloudIdException(){
		try {
			when(client.deleteCloudId("test")).thenThrow(new CloudException("test", new GenericException("test")));
			client.deleteCloudId("test");	
		} catch (Exception e) {
			assertTrue(e.getClass().isAssignableFrom(CloudException.class));
		}
	}
	
	
	private static CloudId createCloudId(){
		CloudId cloudId = new CloudId();
		cloudId.setId(Base36.encode("/test/test"));
		cloudId.setLocalId(createLocalId());
		return cloudId;
	}
	
	private static LocalId createLocalId(){
		LocalId lid = new LocalId();
		lid.setProviderId("test");
		lid.setRecordId("test");
		return lid;
	}
}
