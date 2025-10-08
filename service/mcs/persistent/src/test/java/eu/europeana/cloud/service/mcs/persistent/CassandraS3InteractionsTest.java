package eu.europeana.cloud.service.mcs.persistent;

import static eu.europeana.cloud.service.mcs.Storage.OBJECT_STORAGE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.context.SpiedServicesTestContext;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpiedServicesTestContext.class})
public class CassandraS3InteractionsTest extends CassandraTestBase {

  @Autowired
  private CassandraRecordService cassandraRecordService;

  @Autowired
    private S3ContentDAO s3ContentDAO;

  @Autowired
  private UISClientHandler uisHandler;

  @Autowired
  private CassandraDataSetService cassandraDataSetService;

  private static final String providerId = "provider";

  @After
  public void resetMocks() {
	Mockito.reset(s3ContentDAO);
    Mockito.reset(uisHandler);
  }

  @Test
  public void shouldRemainConsistentWhenS3NotWorks() throws Exception {

    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider(providerId);
    Mockito.doReturn(true).when(uisHandler).existsCloudId("id");
    // prepare failure
	Mockito.doThrow(new MockException()).when(s3ContentDAO)
           .putContent(anyString(), any(InputStream.class));
    // given representation
    DataSet ds = cassandraDataSetService.createDataSet(providerId, "ds_name",
        "description of this set");

    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null, OBJECT_STORAGE);
    Representation r = cassandraRecordService.createRepresentation("id",
        "dc", providerId, UUIDs.timeBased(), "ds_name");

    // when content is put
    try {
      cassandraRecordService.putContent(r.getCloudId(),
          r.getRepresentationName(), r.getVersion(), f,
          new ByteArrayInputStream(dummyContent));
    } catch (MockException e) {
      // it's expected
    }

    // then - no file should be present
    Representation fetched = cassandraRecordService.getRepresentation(
        r.getCloudId(), r.getRepresentationName(), r.getVersion());
    Assert.assertTrue(fetched.getFiles().isEmpty());
  }

  static class MockException extends RuntimeException {

  }

}
