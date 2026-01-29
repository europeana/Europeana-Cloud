package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.ResultActions;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsErrorInfo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_XML;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class UncaughtExceptionMapperTest extends AbstractResourceTest {

  private RecordService recordService;


  @BeforeEach
  void mockUp() {
    recordService = applicationContext.getBean(RecordService.class);
    Mockito.reset(recordService);
  }


  @Test
  void shouldReturnErrorInfoOnEveryException()
          throws Exception {
    Throwable exception = new RuntimeException("error details");
    when(recordService.getRecord(anyString())).thenThrow(exception);

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationsPath("id"))
                    .accept(MediaType.APPLICATION_XML))
            .andExpect(status().isInternalServerError());

    ErrorInfo errorInfo = responseContentAsErrorInfo(response, APPLICATION_XML);
    assertThat(errorInfo.getErrorCode(), is(McsErrorCode.OTHER.toString()));
    assertThat(errorInfo.getDetails(), is(exception.getMessage()));
  }
}
