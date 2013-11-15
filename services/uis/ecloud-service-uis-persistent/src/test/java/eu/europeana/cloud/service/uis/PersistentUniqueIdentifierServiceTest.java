package eu.europeana.cloud.service.uis;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.service.uis.database.Cassandra;
import eu.europeana.cloud.service.uis.database.DatabaseService;
import eu.europeana.cloud.service.uis.database.dao.CloudIdDao;
import eu.europeana.cloud.service.uis.database.dao.LocalIdDao;
import eu.europeana.cloud.service.uis.encoder.Base36;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/default-context.xml"})
@TestExecutionListeners({CassandraUnitTestExecutionListener.class})
@CassandraDataSet(keyspace=Cassandra.KEYSPACE)
@EmbeddedCassandra(host = Cassandra.HOST, port = Cassandra.PORT)
public class PersistentUniqueIdentifierServiceTest {
	private PersistentUniqueIdentifierService service;
	private static DatabaseService dbService;

	@Before
	public void prepare() {
		System.setProperty("storage-config", "src/test/resources");
		try{
		dbService = new DatabaseService(Cassandra.HOST, Integer.toString(Cassandra.PORT), Cassandra.KEYSPACE);
		} catch(IOException e){
			e.printStackTrace();
		}
		CloudIdDao cloudIdDao = new CloudIdDao(dbService);
		LocalIdDao localIdDao = new LocalIdDao(dbService);
		service = new PersistentUniqueIdentifierService(cloudIdDao, localIdDao);

	}

	@Test(expected = RecordExistsException.class)
	public void testCreateAndRetrieve() {
		CloudId gId = service.createGlobalId("test", "test");
		CloudId gIdRet = service.getGlobalId("test", "test");
		assertEquals(gId, gIdRet);
		service.createGlobalId("test", "test");
	}

	@Test(expected = RecordDoesNotExistException.class)
	public void testRecordDoesNotExist() {
		service.getGlobalId("test2", "test2");
	}

	@Test(expected = GlobalIdDoesNotExistException.class)
	public void testGetLocalIdsByGlobalId() {
		List<LocalId> gid = service.getLocalIdsByGlobalId(Base36.encode("/test11/test11"));
		CloudId gId = service.createGlobalId("test11", "test11");
		gid = service.getLocalIdsByGlobalId(gId.getId());
		assertEquals(gid.size(), 1);
	}

	@Test
	public void testGetGlobalIdsByProvider() {
		service.createGlobalId("test3", "test3");
		List<CloudId> cIds = service.getGlobalIdsByProvider("test3", null, 10000);
		assertEquals(cIds.size(), 1);
		cIds = service.getGlobalIdsByProvider("test3", "test3", 1);
		assertEquals(cIds.size(), 1);
	}

	// @Test (expected = RecordDatasetEmptyException.class)
	// public void testGetGlobalIdsByProviderDatasetEmtpy(){
	// service.createGlobalId("test4", "test4");
	// service.getGlobalIdsByProvider("test4", "test5", 1);
	// }

	@Test
	public void testGetLocalIdsByProviderId() {
		service.createGlobalId("test5", "test5");
		List<LocalId> cIds = service.getLocalIdsByProvider("test5", "test5", 1);
		assertEquals(cIds.size(), 1);
		cIds = service.getLocalIdsByProvider("test5", null, 10000);
		assertEquals(cIds.size(), 1);
		cIds = service.getLocalIdsByProvider("test10", null, 10000);
		cIds = service.getLocalIdsByProvider("test10", "test", 1);
	}

	// @Test (expected = RecordDatasetEmptyException.class)
	// public void testGetLocalIdsByProviderDatasetEmtpy(){
	// service.createGlobalId("test6", "test6");
	// service.getLocalIdsByProvider("test6", "test7", 1);
	// }

	@Test(expected = IdHasBeenMappedException.class)
	public void testCreateIdMapping() {
		CloudId gid = service.createGlobalId("test12", "test12");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.createIdMapping(gid.getId(), "test12", "test13");
	}

	@Test(expected = GlobalIdDoesNotExistException.class)
	public void testCreateIdMappingGlobalIdDoesNotExist() {
		service.createGlobalId("test14", "test14");
		service.createIdMapping("test15", "test16", "test17");
	}

	@Test(expected = RecordDoesNotExistException.class)
	public void testRemoveIdMapping() {
		service.createGlobalId("test16", "test16");
		service.removeIdMapping("test16", "test16");
		service.getGlobalId("test16", "test16");
	}

	// @Test (expected = ProviderDoesNotExistException.class)
	// public void testRemoveIdMappingProvDoesNotExist(){
	// service.createGlobalId("test17", "test17");
	// service.removeIdMapping("test18", "test18");
	// }
	//
	// @Test (expected = RecordIdDoesNotExistException.class)
	// public void testRemoveIdMappingRecIdDoesNotExist(){
	// service.createGlobalId("test19", "test19");
	// service.removeIdMapping("test19", "test20");
	// }
	//
	@Test(expected = RecordDoesNotExistException.class)
	public void testDeleteGlobalId() {
		CloudId cId = service.createGlobalId("test21", "test21");
		service.deleteGlobalId(cId.getId());
		service.getGlobalId(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
	}

	@Test(expected = GlobalIdDoesNotExistException.class)
	public void testDeleteGlobalIdException() {
		service.deleteGlobalId("test");
	}

}
