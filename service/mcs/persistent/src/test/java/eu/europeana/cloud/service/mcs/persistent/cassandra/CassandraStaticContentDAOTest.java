package eu.europeana.cloud.service.mcs.persistent.cassandra;

import eu.europeana.cloud.service.mcs.persistent.CassandraTestBase;
import eu.europeana.cloud.service.mcs.persistent.context.CassandraStaticContentDAOTestContext;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAOTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * @author krystian.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {CassandraStaticContentDAOTestContext.class})
public class CassandraStaticContentDAOTest extends ContentDAOTest {

  CassandraTestBase testBase = new CassandraTestBase() {
  };

  @AfterEach
  void tearDown() {
    testBase.truncateAll();
  }
}