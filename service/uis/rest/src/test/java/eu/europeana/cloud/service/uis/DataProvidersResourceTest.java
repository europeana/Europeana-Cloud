package eu.europeana.cloud.service.uis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.response.ResultSlice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.util.AssertionErrors.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * DataProviderResourceTest
 */
@ExtendWith(SpringExtension.class)
@WebAppConfiguration
@ContextConfiguration(classes = {TestConfiguration.class})
class DataProvidersResourceTest {

  MockMvc mockMvc;
  @Autowired
  private WebApplicationContext wac;
  @Autowired
  private DataProviderService dataProviderService;

  /**
   * Test return empty list when provider does not exist
   */
  @Test
  void shouldReturnEmptyListOfProvidersIfNoneExists() throws Exception {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();

    // given there is no provider
    Mockito.doReturn(new ResultSlice<DataProvider>()).when(dataProviderService).getProviders(Mockito.any(), Mockito.anyInt());

    // when you list all providers
    MvcResult response = mockMvc.perform(get("/data-providers").accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andReturn();

    ResultSlice<DataProvider> retrievedInfo = new ObjectMapper().readValue(
        response.getResponse().getContentAsString(), new TypeReference<ResultSlice<DataProvider>>() {
        });

    assertTrue("Expected empty list of data providers", retrievedInfo.getResults().isEmpty());

  }
}
