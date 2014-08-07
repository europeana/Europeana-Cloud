package eu.europeana.cloud.service.uis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.rest.DataProvidersResource;
import eu.europeana.cloud.service.uis.rest.JerseyConfig;

/**
 * DataProviderResourceTest
 */
public class DataProvidersResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private WebTarget dataProvidersWebTarget;

    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:/*-context-test.xml");
    }

    /**
     * Get the mocks form the application context
     */
    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        dataProvidersWebTarget = target(DataProvidersResource.class.getAnnotation(Path.class).value());
    }

    /**
     * Test return empty list when provider does not exist
     */
    @Test
    public void shouldReturnEmptyListOfProvidersIfNoneExists() {
        // given there is no provider
        Mockito.when(dataProviderService.getProviders(Mockito.anyString(), Mockito.anyInt())).thenReturn(
                new ResultSlice<DataProvider>());
        // when you list all providers
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        @SuppressWarnings("unchecked")
        ResultSlice<DataProvider> dataProviders = listDataProvidersResponse.readEntity(ResultSlice.class);

        // then you should get empty list
        assertTrue("Expected empty list of data providers", dataProviders.getResults().isEmpty());
    }

    /**
     * Create a new provider
     *
     * @throws ProviderDoesNotExistException
     * @throws ProviderAlreadyExistsException
     */
    @Test
    public void shouldCreateProvider() throws ProviderDoesNotExistException, ProviderAlreadyExistsException {
        // given certain provider data
        DataProviderProperties properties = new DataProviderProperties();
        properties.setOrganisationName("Organizacja");
        properties.setRemarks("Remarks");
        String providerName = "provident";
        DataProvider dp = new DataProvider();
        dp.setId(providerName);
        dp.setProperties(properties);
        Mockito.when(dataProviderService.createProvider(providerName, properties)).thenReturn(dp);
        Mockito.when(dataProviderService.getProvider(providerName)).thenReturn(dp);
        // when you put the provider into storage
        WebTarget providentWebTarget = dataProvidersWebTarget.queryParam(ParamConstants.F_PROVIDER, providerName);
        Response putResponse = providentWebTarget.request().post(Entity.json(properties));
        assertEquals(Response.Status.CREATED.getStatusCode(), putResponse.getStatus());

        // then the inserted provider should be in service
        DataProvider provider = dataProviderService.getProvider(providerName);
        assertEquals(providerName, provider.getId());
        assertEquals(properties, provider.getProperties());
    }

    /**
     * Return a newly created provider
     *
     * @throws ProviderAlreadyExistsException
     */
    @Test
    public void shouldReturnInsertedProviderOnList() throws ProviderAlreadyExistsException {
        // given one provider in service
        String providerName = "provident";
        ResultSlice<DataProvider> dpSlice = new ResultSlice<>();
        final DataProvider dp = new DataProvider();
        dp.setId(providerName);
        dpSlice.setResults(new ArrayList<DataProvider>() {
            {
                add(dp);
            }
        });
        Mockito.when(dataProviderService.getProviders(Mockito.anyString(), Mockito.anyInt())).thenReturn(
                dpSlice);

        // when you list all providers
        Response listDataProvidersResponse = dataProvidersWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataProvidersResponse.getStatus());
        @SuppressWarnings("unchecked")
        ResultSlice<DataProvider> dataProviders = listDataProvidersResponse.readEntity(ResultSlice.class);

        // then there should be exactly one provider, the same as inserted
        assertEquals("Expected single data provider on list", 1, dataProviders.getResults().size());
        assertEquals("Wrong provider identifier", providerName, dataProviders.getResults().get(0).getId());
    }
}
