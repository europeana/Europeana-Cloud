package eu.europeana.ecloud.service.uis;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.InMemoryUniqueIdentifierService;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;

/**
 * Unit test for the In Memory i0mplementation of the database
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class InMemoryUniqueIdentifierServiceTest {

	InMemoryUniqueIdentifierService service;
	
	/**
	 * Prepare the test execution
	 */
	@Before
	public void prepare(){
		service = new InMemoryUniqueIdentifierService();
	}
	
	/**
	 * Test the creation and retrieval of an object and the exception that a record exists
	 * @throws Exception 
	 */
	@Test (expected = RecordExistsException.class)
	public void testCreateAndRetrieve()throws Exception{
		CloudId gId = service.createCloudId("test", "test");
		CloudId gIdRet = service.getCloudId("test", "test");
		assertEquals(gId,gIdRet);
		service.createCloudId("test", "test");
		service.reset();
	}
	
	/**
	 * Test that a record does not exist
	 * @throws Exception 
	 */
	@Test (expected = RecordDoesNotExistException.class)
	public void testRecordDoesNotExist()throws Exception{
		service.getCloudId("test2", "test2");
		service.reset();
	}
	
	/**
	 * Test CloudId retrieval and exception if it does not exist
	 * @throws Exception 
	 */
	@Test(expected = CloudIdDoesNotExistException.class)
	public void testGetLocalIdsByCloudId()throws Exception{
		List<LocalId> gid = service.getLocalIdsByCloudId(Base36.encode("/test11/test11"));
		CloudId gId = service.createCloudId("test11", "test11");
		gid = service.getLocalIdsByCloudId(gId.getId());
		assertEquals(gid.size(),1);
		service.reset();
	}
	
	/**
	 * Test retrieval by a provider id and exception if it does not exist
	 * @throws Exception 
	 */
	@Test (expected = ProviderDoesNotExistException.class)
	public void testGetCloudIdsByProvider()throws Exception{
		service.createCloudId("test3", "test3");
		List<CloudId> cIds = service.getCloudIdsByProvider("test3", "test3", 1);
		assertEquals(cIds.size(),1);
		cIds = service.getCloudIdsByProvider("test3",null,10000);
		assertEquals(cIds.size(),1);
		cIds = service.getCloudIdsByProvider("test9",null,10000);
		cIds = service.getCloudIdsByProvider("test9", "test", 1);
		service.reset();
	}
	
	/**
	 * Test if a dataset is empty
	 * @throws Exception 
	 */
	@Test (expected = RecordDatasetEmptyException.class)
	public void testGetCloudIdsByProviderDatasetEmtpy()throws Exception{
		service.createCloudId("test4", "test4");
		service.getCloudIdsByProvider("test4", "test5", 1);
		service.reset();
	}
	
	/**
	 * Test localId retrieval and exception if the database does not exist
	 * @throws Exception 
	 */
	@Test (expected = ProviderDoesNotExistException.class)
	public void testGetLocalIdsByProviderId()throws Exception{
		service.createCloudId("test5", "test5");
		List<LocalId> cIds = service.getLocalIdsByProvider("test5", "test5", 1);
		assertEquals(cIds.size(),1);
		cIds = service.getLocalIdsByProvider("test5",null,10000);
		assertEquals(cIds.size(),1);
		cIds = service.getLocalIdsByProvider("test10",null,10000);
		cIds = service.getLocalIdsByProvider("test10", "test", 1);
		service.reset();
	}
	/**
	 * Test if a dataset is empty
	 * @throws Exception 
	 */
	@Test (expected = RecordDatasetEmptyException.class)
	public void testGetLocalIdsByProviderDatasetEmtpy()throws Exception{
		service.createCloudId("test6", "test6");
		service.getLocalIdsByProvider("test6", "test7", 1);
		service.reset();
	}
	
	/**
	 * Test Create mapping and exception if the record Id is already mapped
	 * @throws Exception 
	 */
	@Test (expected = IdHasBeenMappedException.class)
	public void testCreateIdMapping()throws Exception{
		CloudId gid = service.createCloudId("test12", "test12");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.reset();
	}
	
	/**
	 * Test create mapping if the cloud Id does not exist
	 * @throws Exception 
	 */
	@Test(expected = CloudIdDoesNotExistException.class)
	public void testCreateIdMappingCloudIdDoesNotExist() throws Exception{
		service.createCloudId("test14", "test14");
		service.createIdMapping("test15", "test16", "test17");
		service.reset();
	}
	
	/**
	 * Test mapping removal
	 * @throws Exception 
	 */
	@Test (expected = RecordDoesNotExistException.class)
	public void testRemoveIdMapping()throws Exception{
		service.createCloudId("test16", "test16");
		service.removeIdMapping("test16", "test16");
		service.getCloudId("test16", "test16");
		service.reset();
	}
	
	/**
	 * Test mapping removal provider does not exist
	 * @throws Exception 
	 */
	@Test (expected = ProviderDoesNotExistException.class)
	public void testRemoveIdMappingProvDoesNotExist()throws Exception{
		service.createCloudId("test17", "test17");
		service.removeIdMapping("test18", "test18");
		service.reset();
	}
	
	/**
	 * Test mapping removal record does not exist
	 * @throws Exception 
	 */
	@Test (expected = RecordIdDoesNotExistException.class)
	public void testRemoveIdMappingRecIdDoesNotExist()throws Exception{
		service.createCloudId("test19", "test19");
		service.removeIdMapping("test19", "test20");
		service.reset();
	}
	
	/**
	 * Test cloud id deletion
	 * @throws Exception 
	 */
	@Test (expected = RecordDoesNotExistException.class)
	public void testDeleteCloudId()throws Exception{
		CloudId cId = service.createCloudId("test21", "test21");
		service.deleteCloudId(cId.getId());
		service.getCloudId(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
		service.reset();
	}
	
	/**
	 * Test cloud id deletion exception
	 * @throws Exception 
	 */
	@Test (expected = CloudIdDoesNotExistException.class)
	public void testDeleteCloudIdException()throws Exception{
		service.deleteCloudId("test");
		service.reset();
	}
}
