package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.rest.DataProviderActivationResource;
import eu.europeana.cloud.service.uis.rest.JerseyConfig;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

/**
 * Tests for DataProviderActivationResource.class
 */
public class DataProviderActivationResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private WebTarget dataProviderActivationWebTarget;

    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation",
                "classpath:/uis-context-test.xml");
    }

    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils
                .getApplicationContext();
        dataProviderService = applicationContext
                .getBean(DataProviderService.class);
        Mockito.reset(dataProviderService);
        dataProviderActivationWebTarget = target(DataProviderActivationResource.class
                .getAnnotation(Path.class).value());
    }

    @Test
    public void shoudDeactivateDataProvider() throws ProviderAlreadyExistsException, ProviderDoesNotExistException {
        DataProvider dp = new DataProvider();
        Mockito.when(
                dataProviderService.getProvider(Mockito.anyString())).thenReturn(dp);
        Mockito.when(
                dataProviderService.updateProvider(Mockito.any(DataProvider.class))).thenReturn(dp);

        WebTarget providerWebTarget = dataProviderActivationWebTarget.resolveTemplate(
                ParamConstants.P_PROVIDER, "sampleProvider");
        Response deleteResponse = providerWebTarget.request().delete();
        Assert.assertEquals(deleteResponse.getStatus(), 200);

    }

    @Test
    public void shouldThrowExceptionWhenProviderDoesNotExists() throws Exception {
        DataProvider dp = new DataProvider();
        Mockito.when(
                dataProviderService.getProvider(Mockito.anyString())).thenThrow(
                new ProviderDoesNotExistException(new IdentifierErrorInfo(
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getHttpCode(),
                        IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                                .getErrorInfo("provident"))));
        Mockito.when(
                dataProviderService.updateProvider(Mockito.any(DataProvider.class))).thenReturn(dp);

        WebTarget providerWebTarget = dataProviderActivationWebTarget.resolveTemplate(
                ParamConstants.P_PROVIDER, "sampleProvider");
        Response deleteResponse = providerWebTarget.request().delete();
        Assert.assertEquals(deleteResponse.getStatus(), 404);
    }
}
