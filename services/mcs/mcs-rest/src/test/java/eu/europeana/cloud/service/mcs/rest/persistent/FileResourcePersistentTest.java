package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.rest.*;
import javax.ws.rs.core.Application;

/**
 * FileResourceTest
 */
public class FileResourcePersistentTest extends FileResourceTest {

	@Override
	public Application configure() {
		return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
	}

}
