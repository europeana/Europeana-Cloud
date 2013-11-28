package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.rest.*;
import eu.europeana.cloud.test.CassandraTestRunner;
import javax.ws.rs.core.Application;
import org.junit.After;
import org.junit.runner.RunWith;

/**
 * DataSetResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetsResourcePersistentTest extends DataSetsResourceTest {

	@After
	@Override
	public void cleanUp() {
	}


	@Override
	public Application configure() {
		return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
	}
}
