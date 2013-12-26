package eu.europeana.cloud.service.uis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.DataProvidersResource;
import eu.europeana.cloud.service.uis.rest.JerseyConfig;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import eu.europeana.cloud.common.web.ParamConstants;
/**
 * DataProviderResourceTest
 */
public class DataProviderResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;


    private WebTarget dataProvidersWebTarget;

    private WebTarget dataProviderWebTarget;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:/ecloud-uidservice-context-test.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        Mockito.reset(dataProviderService);
        dataProvidersWebTarget = target(DataProvidersResource.class.getAnnotation(Path.class).value());
        dataProviderWebTarget = target(DataProviderResource.class.getAnnotation(Path.class).value());
    }


    


    @Test
    public void shouldUpdateProvider()
            throws ProviderAlreadyExistsException, ProviderDoesNotExistException {
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
    public void shouldGetProvider()
            throws ProviderAlreadyExistsException {
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
        assertEquals(IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo().getErrorCode(),deleteErrorInfo.getErrorCode());
    }


  
}
