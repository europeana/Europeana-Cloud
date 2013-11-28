package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.rest.*;
import eu.europeana.cloud.test.CassandraTestRunner;
import javax.ws.rs.core.Application;
import org.junit.After;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * FileResourceTest
 */
@RunWith(CassandraTestRunner.class)
@Ignore
public class FileResourcePersistentTest extends FileResourceTest {

	@Override
	public Application configure() {
		return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
	}
	

	@After
	@Override
	public void cleanUp() {
	}

}
