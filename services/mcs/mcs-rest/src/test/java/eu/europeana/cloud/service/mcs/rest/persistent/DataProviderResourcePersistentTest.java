package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.rest.DataProviderResourceTest;
import eu.europeana.cloud.service.mcs.rest.JerseyConfig;
import eu.europeana.cloud.test.CassandraTestRunner;
import javax.ws.rs.core.Application;
import org.junit.After;
import org.junit.runner.RunWith;

/**
 *
 * @author sielski
 */
@RunWith(CassandraTestRunner.class)
public class DataProviderResourcePersistentTest extends DataProviderResourceTest {

	@After
	@Override
	public void cleanUp() {
	}


	@Override
	public Application configure() {
		return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
	}
}
