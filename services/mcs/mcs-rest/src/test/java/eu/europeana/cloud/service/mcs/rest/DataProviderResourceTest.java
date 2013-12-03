package eu.europeana.cloud.service.mcs.rest;


import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
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
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * DataProviderResourceTest
 */
public class DataProviderResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private DataSetService dataSetService;

    private WebTarget dataProvidersWebTarget;

    private WebTarget dataProviderWebTarget;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        dataSetService = applicationContext.getBean(DataSetService.class);

        dataProvidersWebTarget = target(DataProvidersResource.class.getAnnotation(Path.class).value());
        dataProviderWebTarget = target(DataProviderResource.class.getAnnotation(Path.class).value());
    }


    @After
    public void cleanUp() {
        for (DataProvider prov : dataProviderService.getProviders(null, 10000).getResults()) {
            dataProviderService.deleteProvider(prov.getId());
        }
    }


    @Test
	public void shouldUpdateProvider() {
        // given certain provider data
        String providerName = "provident";
		dataProviderService.createProvider(providerName, new DataProviderProperties());

		// when the provider is updated
		DataProviderProperties properties = new DataProviderProperties();
		properties.setOrganisationName("Organizacja");
		properties.setRemarks("Remarks");
		WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName);
		Response putResponse = providentWebTarget.request().put(Entity.json(properties));
		assertEquals(Response.Status.NO_CONTENT.getStatusCode(), putResponse.getStatus());

		// then the inserted provider should be in service
		DataProvider provider = dataProviderService.getProvider(providerName);
		assertEquals(providerName, provider.getId());
		assertEquals(properties, provider.getProperties());
    }


    @Test
    public void shouldGetProvider() {
        // given certain provider in service
        DataProviderProperties properties = new DataProviderProperties();
        properties.setOrganisationName("Organizacja");
        properties.setRemarks("Remarks");
        String providerName = "provident";
        dataProviderService.createProvider(providerName, properties);

        // when you get provider by rest api
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName);
        Response getResponse = providentWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getResponse.getStatus());
        DataProvider receivedDataProvider = getResponse.readEntity(DataProvider.class);

        // then received provider should be the same as inserted
        assertEquals(providerName, receivedDataProvider.getId());
        assertEquals(properties, receivedDataProvider.getProperties());
    }


    @Test
    public void shouldReturn404OnNotExistingProvider() {
        // given there is no provider in service

        // when you get certain provider
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, "provident");
        Response getResponse = providentWebTarget.request().get();

        // then you should get error that such does not exist
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), getResponse.getStatus());
        ErrorInfo deleteErrorInfo = getResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.PROVIDER_NOT_EXISTS.toString(), deleteErrorInfo.getErrorCode());
    }

    @Test
    public void shouldDeleteProvider() {
        // given a certain provider in service
        String providerName = "provident";
        dataProviderService.createProvider(providerName, new DataProviderProperties());

        // when you delete it
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName);
        Response deleteResponse = providentWebTarget.request().delete();
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), deleteResponse.getStatus());

        // then it should not be in service anymore
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        ResultSlice<DataProvider> dataProviders = listDataProvidersResponse.readEntity(ResultSlice.class);
        assertTrue("Expected empty list of data providers", dataProviders.getResults().isEmpty());
    }
}
