package eu.europeana.cloud.service.mcs.rest;


import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import javax.ws.rs.client.Entity;

/**
 * DataProviderResourceTest
 */
//@Ignore

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
        dataProvidersWebTarget = target(DataProvidersResource.class.getAnnotation(Path.class).value());
    }


	@After
	public void cleanUp() {
		for (DataProvider prov : dataProviderService.getProviders(null, 10000).getResults()) {
			dataProviderService.deleteProvider(prov.getId());
		}
	}


    @Test
    public void shouldReturnEmptyListOfProvidersIfNoneExists() {
        // given there is no provider

        // when you list all providers 
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        ResultSlice<DataProvider> dataProviders= listDataProvidersResponse.readEntity(ResultSlice.class);

        // then you should get empty list
        assertTrue("Expected empty list of data providers", dataProviders.getResults().isEmpty());
	}


	@Test
	public void shouldCreateProvider() {
		// given certain provider data
		DataProviderProperties properties = new DataProviderProperties();
		properties.setOrganisationName("Organizacja");
		properties.setRemarks("Remarks");
		String providerName = "provident";

		// when you put the provider into storage
		WebTarget providentWebTarget = dataProvidersWebTarget.queryParam(ParamConstants.F_PROVIDER, providerName);
		Response putResponse = providentWebTarget.request().post(Entity.json(properties));
		assertEquals(Response.Status.CREATED.getStatusCode(), putResponse.getStatus());

		// then the inserted provider should be in service
		DataProvider provider = dataProviderService.getProvider(providerName);
		assertEquals(providerName, provider.getId());
		assertEquals(properties, provider.getProperties());
	}


    @Test
    public void shouldReturnInsertedProviderOnList() {
        // given one provider in service
        String providerName = "provident";
        dataProviderService.createProvider(providerName, new DataProviderProperties());

        // when you list all providers 
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        ResultSlice<DataProvider> dataProviders = listDataProvidersResponse.readEntity(ResultSlice.class);
		
        // then there should be exacly one provider, the same as inserted
        assertEquals("Expected single data provider on list", 1, dataProviders.getResults().size());
        assertEquals("Wrong provider identifier", providerName, dataProviders.getResults().get(0).getId());
    }
}
