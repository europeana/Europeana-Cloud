package eu.europeana.cloud.service.uis;

import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;

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
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.JerseyConfig;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
/**
 * DataProviderResourceTest
 */
public class DataProviderResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;


    private WebTarget dataProviderWebTarget;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:/ecloud-uidservice-context-test.xml");
    }

    
    /**
     * Retrieve the spring enabled mockups from the application context
     */
    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        Mockito.reset(dataProviderService);
        dataProviderWebTarget = target(DataProviderResource.class.getAnnotation(Path.class).value());
    }


    


    /**
     * Update a provider
     * @throws ProviderAlreadyExistsException
     * @throws ProviderDoesNotExistException
     * @throws MalformedURLException
     */
    @Test
    public void shouldUpdateProvider()
            throws ProviderAlreadyExistsException, ProviderDoesNotExistException, MalformedURLException {
        // given certain provider data
        String providerName = "provident";
        
        DataProvider dp = new DataProvider();
        dp.setId(providerName);
        // when the provider is updated
        DataProviderProperties properties = new DataProviderProperties();
        properties.setOrganisationName("Organizacja");
        properties.setRemarks("Remarks");
        dp.setProperties(properties);
        Mockito.when(dataProviderService.updateProvider(providerName, properties)).thenReturn(dp);
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName);
        Response putResponse = providentWebTarget.request().put(Entity.json(properties));
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), putResponse.getStatus());
        dp.setProperties(properties);
        Mockito.when(dataProviderService.getProvider(providerName)).thenReturn(dp);
        // then the inserted provider should be in service
        DataProvider retProvider = dataProviderService.getProvider(providerName);
        assertEquals(providerName, retProvider.getId());
        assertEquals(properties, retProvider.getProperties());
    }

    /**
     * Get provider Unit tests
     * @throws ProviderAlreadyExistsException
     * @throws ProviderDoesNotExistException
     */
    @Test
    public void shouldGetProvider()
            throws ProviderAlreadyExistsException, ProviderDoesNotExistException {
        // given certain provider in service
        DataProviderProperties properties = new DataProviderProperties();
        properties.setOrganisationName("Organizacja");
        properties.setRemarks("Remarks");
        String providerName = "provident";
        
        DataProvider dp = new DataProvider();
        dp.setId(providerName);
        
        dp.setProperties(properties);
        Mockito.when(dataProviderService.getProvider(providerName)).thenReturn(dp);
        //dataProviderService.createProvider(providerName, properties);

        // when you get provider by rest api
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, providerName);
        Response getResponse = providentWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getResponse.getStatus());
        DataProvider receivedDataProvider = getResponse.readEntity(DataProvider.class);

        // then received provider should be the same as inserted
        assertEquals(providerName, receivedDataProvider.getId());
        assertEquals(properties, receivedDataProvider.getProperties());
    }


    /**
     * Test Non Existing provider
     * @throws ProviderDoesNotExistException
     */
    @Test
    public void shouldReturn404OnNotExistingProvider() throws ProviderDoesNotExistException {
        // given there is no provider in service
    	Mockito.when(dataProviderService.getProvider("provident")).thenThrow( new ProviderDoesNotExistException(new IdentifierErrorInfo(
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
				IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo("provident"))));
        // when you get certain provider
        WebTarget providentWebTarget = dataProviderWebTarget.resolveTemplate(ParamConstants.P_PROVIDER, "provident");
        Response getResponse = providentWebTarget.request().get();

        // then you should get error that such does not exist
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), getResponse.getStatus());
        ErrorInfo deleteErrorInfo = getResponse.readEntity(ErrorInfo.class);
        assertEquals(IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo("provident").getErrorCode(),deleteErrorInfo.getErrorCode());
    }


  
}
