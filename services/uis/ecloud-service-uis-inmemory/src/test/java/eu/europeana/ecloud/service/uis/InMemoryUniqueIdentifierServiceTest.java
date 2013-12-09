package eu.europeana.ecloud.service.uis;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.CloudIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.InMemoryUniqueIdentifierService;
import eu.europeana.cloud.service.uis.encoder.Base36;

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
	 */
	@Test (expected = RecordExistsException.class)
	public void testCreateAndRetrieve(){
		CloudId gId = service.createCloudId("test", "test");
		CloudId gIdRet = service.getCloudId("test", "test");
		assertEquals(gId,gIdRet);
		service.createCloudId("test", "test");
		service.reset();
	}
	
	/**
	 * Test that a record does not exist
	 */
	@Test (expected = RecordDoesNotExistException.class)
	public void testRecordDoesNotExist(){
		service.getCloudId("test2", "test2");
		service.reset();
	}
	
	/**
	 * Test CloudId retrieval and exception if it does not exist
	 */
	@Test(expected = CloudIdDoesNotExistException.class)
	public void testGetLocalIdsByCloudId(){
		List<LocalId> gid = service.getLocalIdsByCloudId(Base36.encode("/test11/test11"));
		CloudId gId = service.createCloudId("test11", "test11");
		gid = service.getLocalIdsByCloudId(gId.getId());
		assertEquals(gid.size(),1);
		service.reset();
	}
	
	/**
	 * Test retrieval by a provider id and exception if it does not exist
	 */
	@Test (expected = ProviderDoesNotExistException.class)
	public void testGetCloudIdsByProvider(){
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
	 */
	@Test (expected = RecordDatasetEmptyException.class)
	public void testGetCloudIdsByProviderDatasetEmtpy(){
		service.createCloudId("test4", "test4");
		service.getCloudIdsByProvider("test4", "test5", 1);
		service.reset();
	}
	
	/**
	 * Test localId retrieval and exception if the database does not exist
	 */
	@Test (expected = ProviderDoesNotExistException.class)
	public void testGetLocalIdsByProviderId(){
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
	 */
	@Test (expected = RecordDatasetEmptyException.class)
	public void testGetLocalIdsByProviderDatasetEmtpy(){
		service.createCloudId("test6", "test6");
		service.getLocalIdsByProvider("test6", "test7", 1);
		service.reset();
	}
	
	/**
	 * Test Create mapping and exception if the record Id is already mapped
	 */
	@Test (expected = IdHasBeenMappedException.class)
	public void testCreateIdMapping(){
		CloudId gid = service.createCloudId("test12", "test12");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.reset();
	}
	
	/**
	 * Test create mapping if the cloud Id does not exist
	 */
	@Test(expected = CloudIdDoesNotExistException.class)
	public void testCreateIdMappingCloudIdDoesNotExist(){
		service.createCloudId("test14", "test14");
		service.createIdMapping("test15", "test16", "test17");
		service.reset();
	}
	
	/**
	 * Test mapping removal
	 */
	@Test (expected = RecordDoesNotExistException.class)
	public void testRemoveIdMapping(){
		service.createCloudId("test16", "test16");
		service.removeIdMapping("test16", "test16");
		service.getCloudId("test16", "test16");
		service.reset();
	}
	
	/**
	 * Test mapping removal provider does not exist
	 */
	@Test (expected = ProviderDoesNotExistException.class)
	public void testRemoveIdMappingProvDoesNotExist(){
		service.createCloudId("test17", "test17");
		service.removeIdMapping("test18", "test18");
		service.reset();
	}
	
	/**
	 * Test mapping removal record does not exist
	 */
	@Test (expected = RecordIdDoesNotExistException.class)
	public void testRemoveIdMappingRecIdDoesNotExist(){
		service.createCloudId("test19", "test19");
		service.removeIdMapping("test19", "test20");
		service.reset();
	}
	
	/**
	 * Test cloud id deletion
	 */
	@Test (expected = RecordDoesNotExistException.class)
	public void testDeleteCloudId(){
		CloudId cId = service.createCloudId("test21", "test21");
		service.deleteCloudId(cId.getId());
		service.getCloudId(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
		service.reset();
	}
	
	/**
	 * Test cloud id deletion exception
	 */
	@Test (expected = CloudIdDoesNotExistException.class)
	public void testDeleteCloudIdException(){
		service.deleteCloudId("test");
		service.reset();
	}
}
