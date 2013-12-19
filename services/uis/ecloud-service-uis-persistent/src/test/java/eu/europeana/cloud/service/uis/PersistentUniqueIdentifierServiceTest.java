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
import eu.europeana.cloud.service.uis.database.Cassandra;
import eu.europeana.cloud.service.uis.database.DatabaseService;
import eu.europeana.cloud.service.uis.database.dao.CloudIdDao;
import eu.europeana.cloud.service.uis.database.dao.LocalIdDao;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;

/**
 * Persistent Unique Identifier Service Unit tests
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/default-context.xml" })
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(keyspace = Cassandra.KEYSPACE)
@EmbeddedCassandra(host = Cassandra.HOST, port = Cassandra.PORT)
public class PersistentUniqueIdentifierServiceTest {
	private PersistentUniqueIdentifierService service;
	private static DatabaseService dbService;

	/**
	 * Prepare method
	 */
	@Before
	public void prepare() {
		System.setProperty("storage-config", "src/test/resources");
		try {
			dbService = new DatabaseService(Cassandra.HOST, Integer.toString(Cassandra.PORT), Cassandra.KEYSPACE,"","");
		} catch (IOException e) {
			e.printStackTrace();
		}
		CloudIdDao cloudIdDao = new CloudIdDao(dbService);
		LocalIdDao localIdDao = new LocalIdDao(dbService);
		service = new PersistentUniqueIdentifierService(cloudIdDao, localIdDao);

	}

	/**
	 * Test RecordExistsException
	 * @throws Exception
	 */
	@Test(expected = RecordExistsException.class)
	public void testCreateAndRetrieve() throws Exception{
		CloudId gId = service.createCloudId("test", "test");
		CloudId gIdRet = service.getCloudId("test", "test");
		assertEquals(gId, gIdRet);
		service.createCloudId("test", "test");
	}

	/**
	 * Test RecordDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = RecordDoesNotExistException.class)
	public void testRecordDoesNotExist() throws Exception{
		service.getCloudId("test2", "test2");
	}

	/**
	 * Test CloudIdDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = CloudIdDoesNotExistException.class)
	public void testGetLocalIdsByCloudId() throws Exception{
		List<LocalId> gid = service.getLocalIdsByCloudId(Base36.encode("/test11/test11"));
		CloudId gId = service.createCloudId("test11", "test11");
		gid = service.getLocalIdsByCloudId(gId.getId());
		assertEquals(gid.size(), 1);
	}

	/**
	 * Test CloudIds by provider
	 * @throws Exception
	 */
	@Test
	public void testGetCloudIdsByProvider()throws Exception {
		service.createCloudId("test3", "test3");
		List<CloudId> cIds = service.getCloudIdsByProvider("test3", null, 10000);
		assertEquals(cIds.size(), 1);
		cIds = service.getCloudIdsByProvider("test3", "test3", 1);
		assertEquals(cIds.size(), 1);
	}

	
	// @Test (expected = RecordDatasetEmptyException.class)
	// public void testGetCloudIdsByProviderDatasetEmtpy(){
	// service.createCloudId("test4", "test4");
	// service.getCloudIdsByProvider("test4", "test5", 1);
	// }

	/**
	 * Test LocalIds by provider
	 * @throws Exception
	 */
	@Test
	public void testGetLocalIdsByProviderId() throws Exception{
		service.createCloudId("test5", "test5");
		List<LocalId> cIds = service.getLocalIdsByProvider("test5", "test5", 1);
		assertEquals(cIds.size(), 1);
		cIds = service.getLocalIdsByProvider("test5", null, 10000);
		assertEquals(cIds.size(), 1);
		cIds = service.getLocalIdsByProvider("test10", null, 10000);
		cIds = service.getLocalIdsByProvider("test10", "test", 1);
	}

	
	// @Test (expected = RecordDatasetEmptyException.class)
	// public void testGetLocalIdsByProviderDatasetEmtpy(){
	// service.createCloudId("test6", "test6");
	// service.getLocalIdsByProvider("test6", "test7", 1);
	// }

	/**
	 * Test IdHasBeenMappedException
	 * @throws Exception
	 */
	@Test(expected = IdHasBeenMappedException.class)
	public void testCreateIdMapping()throws Exception {
		CloudId gid = service.createCloudId("test12", "test12");
		service.createIdMapping(gid.getId(), "test12", "test13");
		service.createIdMapping(gid.getId(), "test12", "test13");
	}

	/**
	 * Test CloudIdDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = CloudIdDoesNotExistException.class)
	public void testCreateIdMappingCloudIdDoesNotExist()throws Exception {
		service.createCloudId("test14", "test14");
		service.createIdMapping("test15", "test16", "test17");
	}

	/**
	 * Test RecordDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = RecordDoesNotExistException.class)
	public void testRemoveIdMapping() throws Exception{
		service.createCloudId("test16", "test16");
		service.removeIdMapping("test16", "test16");
		service.getCloudId("test16", "test16");
	}

	// @Test (expected = ProviderDoesNotExistException.class)
	// public void testRemoveIdMappingProvDoesNotExist(){
	// service.createCloudId("test17", "test17");
	// service.removeIdMapping("test18", "test18");
	// }
	//
	// @Test (expected = RecordIdDoesNotExistException.class)
	// public void testRemoveIdMappingRecIdDoesNotExist(){
	// service.createCloudId("test19", "test19");
	// service.removeIdMapping("test19", "test20");
	// }
	//
	
	/**
	 * Test RecordDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = RecordDoesNotExistException.class)
	public void testDeleteCloudId() throws Exception{
		CloudId cId = service.createCloudId("test21", "test21");
		service.deleteCloudId(cId.getId());
		service.getCloudId(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
	}

	/**
	 * Test CloudIdDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = CloudIdDoesNotExistException.class)
	public void testDeleteCloudIdException()throws Exception {
		service.deleteCloudId("test");
	}

}
