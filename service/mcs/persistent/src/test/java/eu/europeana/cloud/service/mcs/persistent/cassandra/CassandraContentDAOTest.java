package eu.europeana.cloud.service.mcs.persistent.cassandra;

import eu.europeana.cloud.service.mcs.persistent.CassandraTestBase;
import eu.europeana.cloud.service.mcs.persistent.context.CassandraContentDAOTestContext;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAOTest;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author krystian.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {CassandraContentDAOTestContext.class})
public class CassandraContentDAOTest extends ContentDAOTest {

  CassandraTestBase testBase = new CassandraTestBase() {
  };

  @After
  public void tearDown() {
    testBase.truncateAll();
  }
}