package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.context.SpiedServicesTestContext;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Date;

import static java.lang.String.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by pwozniak on 2/20/17.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpiedServicesTestContext.class})
class CassandraRecordDAOTest extends CassandraTestBase {

  private static final String CLOUD_ID = "123";
  private static final String SCHEMA = "schema1";
  private static final String PROVIDER = "provider1";
  private static final UUID VERSION = UUID.fromString(new com.eaio.uuid.UUID().toString());
  private static final String DATASET1 = "set1";
  private static final String DATASET2 = "set2";
  @Autowired
  private CassandraRecordDAO recordDAO;

  @Test
  void shouldAddDatasetToExistingRepresentation(){
    recordDAO.createRepresentation(CLOUD_ID,SCHEMA,PROVIDER,new Date(),VERSION, DATASET1,false);

    recordDAO.addDatasetToRepresentation(CLOUD_ID, SCHEMA, valueOf(VERSION), DATASET2);

    assertEquals(Set.of(DATASET1,DATASET2), recordDAO.getRepresentation(CLOUD_ID,SCHEMA,valueOf(VERSION)).getDatasetIds());
  }
}