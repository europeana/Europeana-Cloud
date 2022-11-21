package eu.europeana.cloud.service.uis.service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Persistent Unique Identifier Service Unit tests
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/default-context.xml"})
public class CassandraUniqueIdentifierServiceTest extends CassandraTestBase {

  @Autowired
  private UniqueIdentifierServiceImpl service;

  @Autowired
  private CassandraDataProviderDAO dataProviderDao;

  /**
   * Prepare the unit tests
   */
  @Before
  public void prepare() {
    @SuppressWarnings("resource")
    ApplicationContext context = new ClassPathXmlApplicationContext(
        "default-context.xml");
    service = (UniqueIdentifierServiceImpl) context.getBean("service");
    dataProviderDao = (CassandraDataProviderDAO) context.getBean("dataProviderDao");
  }

  @Test
  public void testCreateOrUpdateAndRetrieve() throws Exception {
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
   * @throws Exception expected RecordDoesNotExistException
   */
  @Test(expected = RecordDoesNotExistException.class)
  public void testRecordDoesNotExist() throws Exception {
    service.getCloudId("test2", "test2");
  }

  /**
   * Test CloudIdDoesNotExistException
   *
   * @throws Exception expected CloudIdDoesNotExistException
   */
  @Test(expected = CloudIdDoesNotExistException.class)
  public void testGetLocalIdsByCloudId() throws Exception {
    service.getLocalIdsByCloudId(IdGenerator.encodeWithSha256AndBase32("/test11/test11"));
    CloudId gId = service.createCloudId("test11", "test11");
    service.getLocalIdsByCloudId(gId.getId());
  }

  /**
   * @throws Exception If something goes wrong
   */
  @Test
  public void testCreateIdMappingImmutable() throws Exception {
    dataProviderDao.createDataProvider("test12", new DataProviderProperties());
    CloudId gid = service.createCloudId("test12", "test12");
    service.createIdMapping(gid.getId(), "test12", "test13");
    service.createIdMapping(gid.getId(), "test12", "test13");

    assertTrue(true);
  }

  /**
   * Test CloudIdDoesNotExistException
   *
   * @throws Exception If something goes wrong
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
   * CreateCloudId collision test. Related to jira issue ECL-392.
   */
  @Test
  @Ignore(value = "Old style test with interesting code. Long time test")
  public void createCloudIdCollisionTest() throws DatabaseConnectionException, ProviderDoesNotExistException {
    // given
    final Map<String, String> map = new HashMap<>();
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
    assertTrue(true);
  }

}
