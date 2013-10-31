package eu.europeana.cloud.service.mcs.rest;

import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;

/**
 * DataProviderResourceTest
 */
public class DataProvidersResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private WebTarget dataProvidersWebTarget;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        dataProvidersWebTarget = target("data-providers");
    }


    @After
    public void cleanUp() {
        for (DataProvider prov : dataProviderService.getProviders()) {
            dataProviderService.deleteProvider(prov.getId());
        }
    }


    @Test
    public void shouldReturnEmptyListOfProvidersIfNoneExists() {
        // given there is no provider

        // when you list all providers 
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        List<DataProvider> dataProviders = listDataProvidersResponse.readEntity(new GenericType<List<DataProvider>>() {
        });

        // then you should get empty list
        assertTrue("Expected empty list of data providers", dataProviders.isEmpty());
    }


    @Test
    public void shouldReturnInsertedProviderOnList() {
        // given one provider in service
        String providerName = "provident";
        dataProviderService.createProvider(providerName, new DataProviderProperties());

        // when you list all providers 
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        List<DataProvider> dataProviders = listDataProvidersResponse.readEntity(new GenericType<List<DataProvider>>() {
        });
        // then there should be exacly one provider, the same as inserted
        assertEquals("Expected single data provider on list", 1, dataProviders.size());
        assertEquals("Wrong provider identifier", providerName, dataProviders.get(0).getId());
    }
}
