package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.rest.exception.DatabaseConnectionExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.InvalidPasswordExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.InvalidUsernameExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.UserDoesNotExistExceptionMapper;
import eu.europeana.cloud.service.aas.rest.exception.UserExistsExceptionMapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

/**
 * UniqueIdResource unit test
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 23, 2013
 */
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

    /**
     * Initialization of the Unique Identifier service mockup
     */
    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        authenticationService = applicationContext.getBean(AuthenticationService.class);
        Mockito.reset(authenticationService);
    }

    /**
     * Test to create a cloud Id
     *
     * @throws Exception
     */
    @Test
    public void testCreateCloudUser() throws Exception {
        User user = new User(username, password);

        Response response = target("/user").queryParam(AASParamConstants.Q_USER_NAME, username).queryParam(AASParamConstants.Q_PASSWORD, password)
                .request(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML).post(null);
        assertThat(response.getStatus(), is(200));

//        when(authenticationService.getUser(username)).thenReturn(new User(username, password));
//        response = target("/user/" + user.getUsername()).request().get();
//        assertThat(response.getStatus(), is(200));
//
//        User retrieveCreate = response.readEntity(User.class);
//        assertEquals(user.getUsername(), retrieveCreate.getUsername());
//        assertEquals(user.getPassword(), retrieveCreate.getPassword());
    }
}
