package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {TestConfiguration.class})
public class AuthenticationResourceTest {

    private MockMvc mockMvc;

    @Autowired
    private AuthenticationService authenticationService;
    @Autowired
    private WebApplicationContext wac;

    private String username = "test";
    private String password = "test2";

    @Before
    public void mockUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @Test
    public void testCreateCloudUser() throws Exception {

        Mockito.doReturn(new User(username, password)).when(authenticationService).getUser(username);

        mockMvc.perform(post("/create-user")
                .param(AASParamConstants.P_USER_NAME, username)
                .param(AASParamConstants.P_PASS_TOKEN, password))
                .andExpect(status().isOk());
    }
}
