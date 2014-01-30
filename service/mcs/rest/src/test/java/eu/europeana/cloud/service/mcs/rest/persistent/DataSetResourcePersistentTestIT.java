package eu.europeana.cloud.service.mcs.rest.persistent;

import eu.europeana.cloud.service.mcs.rest.DataSetResourceTest;
import eu.europeana.cloud.service.mcs.rest.JerseyConfig;
import eu.europeana.cloud.test.CassandraTestRunner;
import javax.ws.rs.core.Application;
import org.junit.runner.RunWith;

/**
 * DataSetResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetResourcePersistentTestIT extends DataSetResourceTest {

    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }

}
