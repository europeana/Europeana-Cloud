package eu.europeana.cloud.service.uis.persistent;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
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
import eu.europeana.cloud.service.uis.persistent.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.persistent.dao.CassandraCloudIdDAO;
import eu.europeana.cloud.service.uis.persistent.dao.CassandraLocalIdDAO;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.junit.Ignore;

/**
 * Persistent Unique Identifier Service Unit tests
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/default-context.xml" })
public class CassandraUniqueIdentifierServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraUniqueIdentifierService service;
    @Autowired
    private CassandraConnectionProvider dbService;
    @Autowired
    private CassandraDataProviderDAO dataProviderDao;

    @Autowired
    private CassandraLocalIdDAO localIdDao;

    @Autowired
    private CassandraCloudIdDAO cloudIdDao;

    /**
     * Prepare the unit tests
     */
    @Before
    public void prepare() {
	@SuppressWarnings("resource")
	ApplicationContext context = new ClassPathXmlApplicationContext(
		"default-context.xml");
	dbService = (CassandraConnectionProvider) context.getBean("dbService");
	service = (CassandraUniqueIdentifierService) context.getBean("service");
	dataProviderDao = (CassandraDataProviderDAO) context
		.getBean("dataProviderDao");
	localIdDao = (CassandraLocalIdDAO) context.getBean("localIdDao");
	cloudIdDao = (CassandraCloudIdDAO) context.getBean("cloudIdDao");
    }

    /**
     * Test RecordExistsException
     * 
     * @throws Exception
     */
    @Test(expected = RecordExistsException.class)
    public void testCreateAndRetrieve() throws Exception {
	dataProviderDao.createDataProvider("test",
		new DataProviderProperties());
	CloudId gId = service.createCloudId("test", "test");
	CloudId gIdRet = service.getCloudId("test", "test");
	assertEquals(gId, gIdRet);
	service.createCloudId("test", "test");
    }

    /**
     * Test RecordDoesNotExistException
     * 
     * @throws Exception
     */
    @Test(expected = RecordDoesNotExistException.class)
    public void testRecordDoesNotExist() throws Exception {
	service.getCloudId("test2", "test2");
    }

    /**
     * Test CloudIdDoesNotExistException
     * 
     * @throws Exception
     */
    @Test(expected = CloudIdDoesNotExistException.class)
    public void testGetLocalIdsByCloudId() throws Exception {
	List<CloudId> gid = service.getLocalIdsByCloudId(IdGenerator
		.encodeWithSha256AndBase32("/test11/test11"));
	CloudId gId = service.createCloudId("test11", "test11");
	gid = service.getLocalIdsByCloudId(gId.getId());
	assertEquals(gid.size(), 1);
    }

    /**
     * Test CloudIds by provider
     * 
     * @throws Exception
     */
    @Test
    public void testGetCloudIdsByProvider() throws Exception {
	dataProviderDao.createDataProvider("test3",
		new DataProviderProperties());
	service.createCloudId("test3", "test3");
	List<CloudId> cIds = service
		.getCloudIdsByProvider("test3", null, 10000);
	assertEquals(cIds.size(), 1);
	cIds = service.getCloudIdsByProvider("test3", "test3", 1);
	assertEquals(cIds.size(), 1);
    }

    /**
     * Test LocalIds by provider
     * 
     * @throws Exception
     */
    @Test
    public void testGetLocalIdsByProviderId() throws Exception {
	dataProviderDao.createDataProvider("test5",
		new DataProviderProperties());
	service.createCloudId("test5", "test5");
	List<CloudId> cIds = service.getLocalIdsByProvider("test5", "test5", 1);
	assertEquals(cIds.size(), 1);
	cIds = service.getLocalIdsByProvider("test5", null, 10000);
	assertEquals(cIds.size(), 1);

    }

    /**
     * Test IdHasBeenMappedException
     * 
     * @throws Exception
     */
    @Test(expected = IdHasBeenMappedException.class)
    public void testCreateIdMapping() throws Exception {
	dataProviderDao.createDataProvider("test12",
		new DataProviderProperties());
	CloudId gid = service.createCloudId("test12", "test12");
	service.createIdMapping(gid.getId(), "test12", "test13");
	service.createIdMapping(gid.getId(), "test12", "test13");
    }

    /**
     * Test CloudIdDoesNotExistException
     * 
     * @throws Exception
     */
    @Test(expected = CloudIdDoesNotExistException.class)
    public void testCreateIdMappingCloudIdDoesNotExist() throws Exception {
	dataProviderDao.createDataProvider("test14",
		new DataProviderProperties());
	dataProviderDao.createDataProvider("test16",
		new DataProviderProperties());
	service.createCloudId("test14", "test14");
	service.createIdMapping("test15", "test16", "test17");
    }

    /**
     * Test RecordDoesNotExistException
     * 
     * @throws Exception
     */
    @Test(expected = RecordDoesNotExistException.class)
    public void testRemoveIdMapping() throws Exception {
	dataProviderDao.createDataProvider("test16",
		new DataProviderProperties());
	service.createCloudId("test16", "test16");
	service.removeIdMapping("test16", "test16");
	service.getCloudId("test16", "test16");
    }

    /**
     * Test RecordDoesNotExistException
     * 
     * @throws Exception
     */
    @Test(expected = RecordDoesNotExistException.class)
    public void testDeleteCloudId() throws Exception {
	dataProviderDao.createDataProvider("test21",
		new DataProviderProperties());
	CloudId cId = service.createCloudId("test21", "test21");
	service.deleteCloudId(cId.getId());
	service.getCloudId(cId.getLocalId().getProviderId(), cId.getLocalId()
		.getRecordId());
    }

    /**
     * Test CloudIdDoesNotExistException
     * 
     * @throws Exception
     */
    @Test(expected = CloudIdDoesNotExistException.class)
    public void testDeleteCloudIdException() throws Exception {
	service.deleteCloudId("test");
    }

    /**
     * CreateCloudId collision test. Related to jira issue ECL-392. Test might
     * took long time and resource.
     */
    @Test
    @Ignore
    public void createCloudIdCollisonTest() throws DatabaseConnectionException,
	    RecordExistsException, ProviderDoesNotExistException,
	    RecordDatasetEmptyException, CloudIdDoesNotExistException,
	    CloudIdAlreadyExistException {
	// given
	final Map<String, String> map = new HashMap<String, String>();
	dataProviderDao.createDataProvider("testprovider",
		new DataProviderProperties());
	for (BigInteger bigCounter = BigInteger.ONE; bigCounter
		.compareTo(new BigInteger("5000000")) < 0; bigCounter = bigCounter
		.add(BigInteger.ONE)) {
	    final String counterString = bigCounter.toString(32);

	    // when
	    final String encodedId = service.createCloudId("testprovider")
		    .getId();
	    if (map.containsKey(encodedId)) {

		// then
		throw new RuntimeException("bigCounter: " + bigCounter
			+ " | counterString: " + counterString
			+ " | encodedId:" + encodedId
			+ " == collision with ==> " + map.get(encodedId));
	    } else {
		map.put(encodedId, "bigCounter: " + bigCounter
			+ " | counterString: " + counterString
			+ " | encodedId:" + encodedId);
	    }

	}
    }

}
