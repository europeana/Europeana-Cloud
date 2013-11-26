package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.rest.*;
import javax.ws.rs.core.Application;

/**
 * DataSetResourceTest
 */
public class DataSetsResourcePersistentTest extends DataSetsResourceTest {


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }
}
