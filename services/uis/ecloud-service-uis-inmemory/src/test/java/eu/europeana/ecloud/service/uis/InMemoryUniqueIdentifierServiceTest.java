package eu.europeana.ecloud.service.uis;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.InMemoryUniqueIdentifierService;
import eu.europeana.cloud.service.uis.encoder.Base36;

public class InMemoryUniqueIdentifierServiceTest {

	InMemoryUniqueIdentifierService service;
	
	@Before
	public void prepare(){
		service = new InMemoryUniqueIdentifierService();
	}
	
	@Test (expected = RecordExistsException.class)
	public void testCreateAndRetrieve(){
		CloudId gId = service.createGlobalId("test", "test");
		CloudId gIdRet = service.getGlobalId("test", "test");
		assertEquals(gId,gIdRet);
		service.createGlobalId("test", "test");
		service.reset();
	}
	
	@Test (expected = RecordDoesNotExistException.class)
	public void testRecordDoesNotExist(){
		service.getGlobalId("test2", "test2");
		service.reset();
	}
	
	@Test(expected = GlobalIdDoesNotExistException.class)
	public void testGetLocalIdsByGlobalId(){
		List<LocalId> gid = service.getLocalIdsByGlobalId(Base36.encode("/test11/test11"));
		CloudId gId = service.createGlobalId("test11", "test11");
		gid = service.getLocalIdsByGlobalId(gId.getId());
		assertEquals(gid.size(),1);
		service.reset();
	}
	
	@Test (expected = ProviderDoesNotExistException.class)
	public void testGetGlobalIdsByProvider(){
		service.createGlobalId("test3", "test3");
		List<CloudId> cIds = service.getGlobalIdsByProvider("test3", "test3", 1);
		assertEquals(cIds.size(),1);
		cIds = service.getGlobalIdsByProvider("test3",null,10000);
		assertEquals(cIds.size(),1);
		cIds = service.getGlobalIdsByProvider("test9",null,10000);
		cIds = service.getGlobalIdsByProvider("test9", "test", 1);
		service.reset();
	}
	
	@Test (expected = RecordDatasetEmptyException.class)
	public void testGetGlobalIdsByProviderDatasetEmtpy(){
		service.createGlobalId("test4", "test4");
		service.getGlobalIdsByProvider("test4", "test5", 1);
		service.reset();
	}
	
	@Test (expected = ProviderDoesNotExistException.class)
	public void testGetLocalIdsByProviderId(){
		service.createGlobalId("test5", "test5");
		List<LocalId> cIds = service.getLocalIdsByProvider("test5", "test5", 1);
		assertEquals(cIds.size(),1);
		cIds = service.getLocalIdsByProvider("test5",null,10000);
		assertEquals(cIds.size(),1);
		cIds = service.getLocalIdsByProvider("test10",null,10000);
		cIds = service.getLocalIdsByProvider("test10", "test", 1);
		service.reset();
	}
	
	@Test (expected = RecordDatasetEmptyException.class)
	public void testGetLocalIdsByProviderDatasetEmtpy(){
		service.createGlobalId("test6", "test6");
		service.getLocalIdsByProvider("test6", "test7", 1);
		service.reset();
	}
	
	@Test (expected = IdHasBeenMappedException.class)
	public void testCreateIdMapping(){
		CloudId gid = service.createGlobalId("test12", "test12");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.reset();
	}
	
	@Test(expected = GlobalIdDoesNotExistException.class)
	public void testCreateIdMappingGlobalIdDoesNotExist(){
		service.createGlobalId("test14", "test14");
		service.createIdMapping("test15", "test16", "test17");
		service.reset();
	}
	
	@Test (expected = RecordDoesNotExistException.class)
	public void testRemoveIdMapping(){
		service.createGlobalId("test16", "test16");
		service.removeIdMapping("test16", "test16");
		service.getGlobalId("test16", "test16");
		service.reset();
	}
	
	@Test (expected = ProviderDoesNotExistException.class)
	public void testRemoveIdMappingProvDoesNotExist(){
		service.createGlobalId("test17", "test17");
		service.removeIdMapping("test18", "test18");
		service.reset();
	}
	
	@Test (expected = RecordIdDoesNotExistException.class)
	public void testRemoveIdMappingRecIdDoesNotExist(){
		service.createGlobalId("test19", "test19");
		service.removeIdMapping("test19", "test20");
		service.reset();
	}
	
	@Test (expected = RecordDoesNotExistException.class)
	public void testDeleteGlobalId(){
		CloudId cId = service.createGlobalId("test21", "test21");
		service.deleteGlobalId(cId.getId());
		service.getGlobalId(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
		service.reset();
	}
	
	@Test (expected = GlobalIdDoesNotExistException.class)
	public void testDeleteGlobalIdException(){
		service.deleteGlobalId("test");
		service.reset();
	}
}
