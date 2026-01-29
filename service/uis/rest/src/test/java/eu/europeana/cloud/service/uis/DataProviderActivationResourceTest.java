package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests for DataProviderActivationResource.class
 */
@ExtendWith(SpringExtension.class)
@WebAppConfiguration
@ContextConfiguration(classes = {TestConfiguration.class})
public class DataProviderActivationResourceTest {

  MockMvc mockMvc;

  @Autowired
  private WebApplicationContext wac;

  @Autowired
  private DataProviderService dataProviderService;

  @BeforeEach
  void mockUp() {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
  }

  @Test
  void shouldDeactivateDataProvider() throws Exception {
    DataProvider dp = new DataProvider();
    Mockito.doReturn(dp).when(dataProviderService).getProvider(Mockito.anyString());
    Mockito.doReturn(dp).when(dataProviderService).updateProvider(Mockito.any(DataProvider.class));
    ;

    mockMvc.perform(delete("/data-providers/{\" + P_PROVIDER + \"}/active", "sampleProvider"))
            .andExpect(status().isOk());
  }

  @Test
  void shouldThrowExceptionWhenProviderDoesNotExists() throws Exception {
    DataProvider dp = new DataProvider();
    Mockito.doThrow(
            new ProviderDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                            .getHttpCode(),
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
                            .getErrorInfo("provident")))
    ).when(dataProviderService).getProvider(Mockito.anyString());

    Mockito.doReturn(dp).when(dataProviderService).updateProvider(Mockito.any(DataProvider.class));
    mockMvc.perform(
               delete("/data-providers/{\" + P_PROVIDER + \"}/active", "sampleProvider"))
           .andExpect(status().isNotFound());
  }
}
