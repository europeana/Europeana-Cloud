package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import junitparams.JUnitParamsRunner;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;

@RunWith(JUnitParamsRunner.class)
public class UncauchtExceptionMapperTest extends JerseyTest {

    private WebTarget dataProviderWebTarget;

    private DataProviderResource dataProviderResource;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderWebTarget = target(DataProviderResource.class.getAnnotation(Path.class).value());
        dataProviderResource = applicationContext.getBean(DataProviderResource.class);
    }
}
