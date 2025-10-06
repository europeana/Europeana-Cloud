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

import com.google.common.collect.Lists;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
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
public class RepresentationsResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  static final private String globalId = "1";
  static final private String schema = "DC";
  static final private String version = "1.0";
  static final private Record record = new Record(globalId, Lists.newArrayList(new Representation(globalId, schema,
      version, null, null, "DLF", Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
      "2013-01-01", 12345, null)), null, true, new Date(), null, false)));


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
  public void getRepresentations(MediaType mediaType) throws Exception {
    Record expected = new Record(record);
    Representation expectedRepresentation = expected.getRepresentations().get(0);
    expectedRepresentation.setUri(URITools.getVersionUri(getBaseUri(), globalId, schema, version));
    expectedRepresentation.setAllVersionsUri(URITools.getAllVersionsUri(getBaseUri(), globalId, schema));
    expectedRepresentation.setFiles(new ArrayList<File>());
    when(recordService.getRecord(globalId)).thenReturn(new Record(record));

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationsPath(globalId).toString()).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    List<Representation> entity = responseContentAsRepresentationList(response, mediaType);
    assertThat(entity, is(expected.getRepresentations()));
    verify(recordService, times(1)).getRecord(globalId);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void getRepresentationsReturns404IfRecordDoesNotExists()
      throws Exception {
    Throwable exception = new RecordNotExistsException();
    when(recordService.getRecord(globalId)).thenThrow(exception);

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationsPath(globalId))
                                        .accept(MediaType.APPLICATION_XML))
                                    .andExpect(status().isNotFound());

    ErrorInfo errorInfo = responseContentAsErrorInfo(response, MediaType.APPLICATION_XML);
    assertThat(errorInfo.getErrorCode(), is(McsErrorCode.RECORD_NOT_EXISTS.toString()));
    verify(recordService, times(1)).getRecord(globalId);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void getRepresentationsReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get(URITools.getRepresentationsPath(globalId))
               .accept(MEDIA_TYPE_APPLICATION_SVG_XML))
           .andExpect(status().isNotAcceptable());
  }

}
