package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.MEDIA_TYPE_APPLICATION_SVG_XML;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.getBaseUri;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsErrorInfo;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsRepresentationList;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.common.collect.ImmutableList;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

@RunWith(JUnitParamsRunner.class)
public class RepresentationVersionsResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  static final private String GLOBAL_ID = "1";
  static final private String SCHEMA = "DC";
  static final private String VERSION = "1.0";

  private static final String LIST_VERSIONS_PATH = URITools.getListVersionsPath(GLOBAL_ID, SCHEMA).toString();
  static final private List<Representation> REPRESENTATIONS = ImmutableList.of(new Representation(GLOBAL_ID, SCHEMA,
      VERSION, null, null, "DLF", Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
      "2013-01-01", 12345, null)), null, true, new Date(), null, false));


  @Before
  public void mockUp() {
    recordService = applicationContext.getBean(RecordService.class);
    Mockito.reset(recordService);
  }


  @SuppressWarnings("unused")
  private Object[] mimeTypes() {
    return $($(MediaType.APPLICATION_XML), $(MediaType.APPLICATION_JSON));
  }


  @Test
  @Parameters(method = "mimeTypes")
  public void testListVersions(MediaType mediaType)
      throws Exception {
    List<Representation> expected = copy(REPRESENTATIONS);
    Representation expectedRepresentation = expected.get(0);
    URITools.enrich(expectedRepresentation, getBaseUri());
    when(recordService.listRepresentationVersions(GLOBAL_ID, SCHEMA)).thenReturn(copy(REPRESENTATIONS));

    ResultActions response = mockMvc.perform(get(LIST_VERSIONS_PATH).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    List<Representation> entity = responseContentAsRepresentationList(response, mediaType);
    assertThat(entity, is(expected));
    verify(recordService, times(1)).listRepresentationVersions(GLOBAL_ID, SCHEMA);
    verifyNoMoreInteractions(recordService);
  }


  private List<Representation> copy(List<Representation> representations) {
    List<Representation> expected = new ArrayList<>();
    for (Representation representation : representations) {
      expected.add(new Representation(representation));
    }
    return expected;
  }


  @SuppressWarnings("unused")
  private Object[] errors() {
    return $($(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()));
  }


  @Test
  @Parameters(method = "errors")
  public void testListVersionsReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception, String errorCode)
      throws Exception {
    when(recordService.listRepresentationVersions(GLOBAL_ID, SCHEMA)).thenThrow(exception);

    ResultActions response = mockMvc.perform(get(LIST_VERSIONS_PATH).accept(MediaType.APPLICATION_XML))
                                    .andExpect(status().isNotFound());

    ErrorInfo errorInfo = responseContentAsErrorInfo(response, org.springframework.http.MediaType.APPLICATION_XML);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).listRepresentationVersions(GLOBAL_ID, SCHEMA);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void testListVersionsReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get(LIST_VERSIONS_PATH).accept(MEDIA_TYPE_APPLICATION_SVG_XML))
           .andExpect(status().isNotAcceptable());
  }

}
