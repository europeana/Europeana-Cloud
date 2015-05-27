package eu.europeana.cloud.service.aas.rest;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.rest.exception.DatabaseConnectionExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.InvalidPasswordExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.InvalidUsernameExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.UserDoesNotExistExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.UserExistsExceptionMapper;

public class AuthenticationResourceTest extends JerseyTest {

    private AuthenticationService authenticationService;
    private String username = "test";
    private String password = "test2";

    /**
     * Configuration of the Spring context
     */
    @Override
    public Application configure() {
        return new ResourceConfig()
                .registerClasses(DatabaseConnectionExceptionMapper.class)
                .registerClasses(InvalidPasswordExceptionMapper.class)
                .registerClasses(InvalidUsernameExceptionMapper.class)
                .registerClasses(UserExistsExceptionMapper.class)
                .registerClasses(UserDoesNotExistExceptionMapper.class)
                .registerClasses(AuthenticationResource.class)
                .property("contextConfigLocation", "classpath:ecloud-aasservice-context-test.xml");
    }

    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        authenticationService = applicationContext.getBean(AuthenticationService.class);
        Mockito.reset(authenticationService);
    }

    @Test
    public void testCreateCloudUser() throws Exception {
    	
        User user = new User(username, password);
        when(authenticationService.getUser(username)).thenReturn(new User(username, password));

        Response response = target("/create-user").queryParam(AASParamConstants.P_USER_NAME, username)
        		.queryParam(AASParamConstants.P_USER_NAME, password)
                .request().post(Entity.json(""));

        assertThat(response.getStatus(), is(200));
    }
}
