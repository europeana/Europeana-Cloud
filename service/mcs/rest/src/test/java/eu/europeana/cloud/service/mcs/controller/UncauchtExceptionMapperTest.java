package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsErrorInfo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_XML;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import javax.ws.rs.core.MediaType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.ResultActions;

public class UncauchtExceptionMapperTest extends AbstractResourceTest {

  private RecordService recordService;


  @Before
  public void mockUp() {
    recordService = applicationContext.getBean(RecordService.class);
    Mockito.reset(recordService);
  }


  @Test
  public void shouldReturnErrorInfoOnEveryException()
      throws Exception {
    Throwable exception = new RuntimeException("error details");
    when(recordService.getRecord(Matchers.anyString())).thenThrow(exception);

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationsPath("id"))
                                        .accept(MediaType.APPLICATION_XML))
                                    .andExpect(status().isInternalServerError());

    ErrorInfo errorInfo = responseContentAsErrorInfo(response, APPLICATION_XML);
    assertThat(errorInfo.getErrorCode(), is(McsErrorCode.OTHER.toString()));
    assertThat(errorInfo.getDetails(), is(exception.getMessage()));
  }
}
