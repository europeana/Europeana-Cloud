package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.rest.DataProviderResourceTest;
import eu.europeana.cloud.service.mcs.rest.JerseyConfig;
import eu.europeana.cloud.test.CassandraTestRunner;
import javax.ws.rs.core.Application;
import org.junit.runner.RunWith;

/**
 *
 * @author sielski
 */
@RunWith(CassandraTestRunner.class)
public class DataProviderResourcePersistentTest extends DataProviderResourceTest {

    @Override
    public Application configure() {
		  return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }
}
