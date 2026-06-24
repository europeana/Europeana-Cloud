package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.context.SpiedServicesTestContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Created by pwozniak on 2/20/17.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpiedServicesTestContext.class})
class CassandraRecordDAOTest extends CassandraTestBase {

  @Autowired
  private CassandraRecordDAO recordDAO;

  @Autowired
  private CassandraRecordService cassandraRecordService;


}