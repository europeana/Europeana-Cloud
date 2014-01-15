package eu.europeana.cloud.service.uis;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.database.DatabaseService;
import eu.europeana.cloud.service.uis.database.dao.CassandraDataProviderDAO;
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
@ContextConfiguration(locations = { "classpath:/default-context.xml" })
public class PersistentUniqueIdentifierServiceTest extends CassandraTestBase {
	@Autowired
	private PersistentUniqueIdentifierService service;
	@Autowired
	private DatabaseService dbService;
	@Autowired
	private CassandraDataProviderDAO dataProviderDao;
	
	@Autowired
	private LocalIdDao localIdDao;
	
	@Autowired
	private CloudIdDao cloudIdDao;
	
	/**
	 * Prepare the unit tests
	 */
	@Before
	public void prepare(){
		@SuppressWarnings("resource")
		ApplicationContext context = new ClassPathXmlApplicationContext("default-context.xml");
		dbService = (DatabaseService) context.getBean("dbService");
		service = (PersistentUniqueIdentifierService) context.getBean("service");
		dataProviderDao = (CassandraDataProviderDAO) context.getBean("dataProviderDao");
		localIdDao = (LocalIdDao) context.getBean("localIdDao");
		cloudIdDao = (CloudIdDao) context.getBean("cloudIdDao");
	}
	
	/**
	 * Test RecordExistsException
	 * @throws Exception
	 */
	@Test(expected = RecordExistsException.class)
	public void testCreateAndRetrieve() throws Exception{
		dataProviderDao.createOrUpdateProvider("test", new DataProviderProperties());
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
		List<CloudId> gid = service.getLocalIdsByCloudId(Base36.encode("/test11/test11"));
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
		dataProviderDao.createOrUpdateProvider("test3", new DataProviderProperties());
		service.createCloudId("test3", "test3");
		List<CloudId> cIds = service.getCloudIdsByProvider("test3", null, 10000);
		assertEquals(cIds.size(), 1);
		cIds = service.getCloudIdsByProvider("test3", "test3", 1);
		assertEquals(cIds.size(), 1);
	}

	/**
	 * Test LocalIds by provider
	 * @throws Exception
	 */
	@Test
	public void testGetLocalIdsByProviderId() throws Exception{
		dataProviderDao.createOrUpdateProvider("test5", new DataProviderProperties());
		service.createCloudId("test5", "test5");
		List<CloudId> cIds = service.getLocalIdsByProvider("test5", "test5", 1);
		assertEquals(cIds.size(), 1);
		cIds = service.getLocalIdsByProvider("test5", null, 10000);
		assertEquals(cIds.size(), 1);
		
	}

	/**
	 * Test IdHasBeenMappedException
	 * @throws Exception
	 */
	@Test(expected = IdHasBeenMappedException.class)
	public void testCreateIdMapping()throws Exception {
		dataProviderDao.createOrUpdateProvider("test12", new DataProviderProperties());
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
		dataProviderDao.createOrUpdateProvider("test14", new DataProviderProperties());
		dataProviderDao.createOrUpdateProvider("test16", new DataProviderProperties());
		service.createCloudId("test14", "test14");
		service.createIdMapping("test15", "test16", "test17");
	}

	/**
	 * Test RecordDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = RecordDoesNotExistException.class)
	public void testRemoveIdMapping() throws Exception{
		dataProviderDao.createOrUpdateProvider("test16", new DataProviderProperties());
		service.createCloudId("test16", "test16");
		service.removeIdMapping("test16", "test16");
		service.getCloudId("test16", "test16");
	}

	/**
	 * Test RecordDoesNotExistException
	 * @throws Exception
	 */
	@Test(expected = RecordDoesNotExistException.class)
	public void testDeleteCloudId() throws Exception{
		dataProviderDao.createOrUpdateProvider("test21", new DataProviderProperties());
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
