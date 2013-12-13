package eu.europeana.cloud.service.mcs.rest.persistent;

import javax.ws.rs.core.Application;

import org.junit.After;
import org.junit.runner.RunWith;

import eu.europeana.cloud.service.mcs.rest.DataSetResourceTest;
import eu.europeana.cloud.service.mcs.rest.JerseyConfig;
import eu.europeana.cloud.test.CassandraTestRunner;

/**
 * DataSetResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetResourcePersistentTestIT extends DataSetResourceTest {

    @After
    @Override
    public void cleanUp() {
    }


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }

}
