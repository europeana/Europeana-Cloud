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
public class DataProviderResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private DataSetService dataSetService;

    WebTarget dataProvidersWebTarget;

    WebTarget dataProviderWebTarget;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        dataSetService = applicationContext.getBean(DataSetService.class);
        dataProvidersWebTarget = target("data-providers");
        dataProviderWebTarget = dataProvidersWebTarget.path("{" + ParamConstants.P_PROVIDER + "}");
    }


    @After
    public void cleanUp() {
        for (DataProvider prov : dataProviderService.getProviders()) {
            dataProviderService.deleteProvider(prov.getId());
        }
    }


    @Test
    public void shouldReturnEmptyListOfProvidersIfNoneExists() {
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        List<DataProvider> dataProviders = listDataProvidersResponse.readEntity(new GenericType<List<DataProvider>>() {
        });
        assertTrue("Expected empty list of data providers", dataProviders.isEmpty());
    }


    @Test
    public void shouldReturnInsertedProviderOnList() {
        DataProviderProperties properties = new DataProviderProperties();
        String providerName = "provident";
        Response putResponse = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName).request().put(Entity.json(properties));
        assertEquals(Response.Status.CREATED.getStatusCode(), putResponse.getStatus());

        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        List<DataProvider> dataProviders = listDataProvidersResponse.readEntity(new GenericType<List<DataProvider>>() {
        });
        assertEquals("Expected single data provider on list", 1, dataProviders.size());
        assertEquals("Wrong provider identifier", providerName, dataProviders.get(0).getId());
    }


    @Test
    public void putAndGetProvider() {
        DataProviderProperties properties = new DataProviderProperties();
        properties.setOrganisationName("Organizacja");
        properties.setRemarks("Remarks");
        String providerName = "provident";
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName);
        Response putResponse = providentWebTarget.request().put(Entity.json(properties));
        assertEquals(Response.Status.CREATED.getStatusCode(), putResponse.getStatus());

        Response getResponse = providentWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getResponse.getStatus());
        DataProvider receivedDataProvider = getResponse.readEntity(DataProvider.class);
        assertEquals(providerName, receivedDataProvider.getId());
        assertEquals(properties, receivedDataProvider.getProperties());
    }


    @Test
    public void shouldDeleteProvider() {
        // put
        String providerName = "provident";
        dataProviderService.createProvider(providerName, new DataProviderProperties());

        // delete
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName);
        Response deleteResponse = providentWebTarget.request().delete();
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), deleteResponse.getStatus());

        // get - should return 404
        Response getResponse = providentWebTarget.request().get();
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), getResponse.getStatus());

        // get list of providers - should be empty
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        List<DataProvider> dataProviders = listDataProvidersResponse.readEntity(new GenericType<List<DataProvider>>() {
        });
        assertTrue("Expected empty list of data providers", dataProviders.isEmpty());
    }
}
