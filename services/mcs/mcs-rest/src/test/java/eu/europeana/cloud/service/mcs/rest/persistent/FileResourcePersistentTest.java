package eu.europeana.cloud.service.mcs.rest.persistent;

import javax.ws.rs.core.Application;

import org.junit.After;
import org.junit.runner.RunWith;

import eu.europeana.cloud.service.mcs.rest.FileResourceTest;
import eu.europeana.cloud.service.mcs.rest.JerseyConfig;
import eu.europeana.cloud.test.CassandraParamertizedTestRunner;

/**
 * FileResourceTest
 */
@RunWith(CassandraParamertizedTestRunner.class)
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
