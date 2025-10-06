package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.common.web.ParamConstants.DATA_SET_ID;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.MEDIA_TYPE_APPLICATION_SVG_XML;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.getBaseUri;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContent;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsErrorInfo;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

@RunWith(JUnitParamsRunner.class)
public class RepresentationResourceTest extends AbstractResourceTest {

  private static final UUID VERSION = UUID.fromString(new com.eaio.uuid.UUID().toString());

  private RecordService recordService;

  static final private String globalId = "1";
  static final private String schema = "DC";
  static final private String version = "1.0";
  static final private String providerID = "DLF";
  static final private Representation representation = new Representation(globalId, schema, version, null, null,
      "DLF", Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
      null)), null, true, new Date(), null, false);

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
  public void getRepresentation(MediaType mediaType)
      throws Exception {
    Representation expected = new Representation(representation);
    expected.setUri(URITools.getVersionUri(getBaseUri(), globalId, schema, version));
    expected.setAllVersionsUri(URITools.getAllVersionsUri(getBaseUri(), globalId, schema));

    ArrayList<File> files = new ArrayList<>(1);
    files.add(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
        "2013-01-01", 12345L, URI.create("http://localhost/records/" + globalId
        + "/representations/" + schema + "/versions/" + version + "/files/1.xml")));

    expected.setFiles(files);
    when(recordService.getRepresentation(globalId, schema)).thenReturn(new Representation(representation));

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationPath(globalId, schema)).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    Representation entity = responseContent(response, Representation.class, mediaType);
    assertThat(entity, is(expected));
    verify(recordService, times(1)).getRepresentation(globalId, schema);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void getRepresentationReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get(URITools.getRepresentationPath(globalId, schema))
               .accept(MEDIA_TYPE_APPLICATION_SVG_XML))
           .andExpect(status().isNotAcceptable());
  }

  @SuppressWarnings("unused")
  private Object[] recordErrors() {
    return $($(new RecordNotExistsException(), McsErrorCode.RECORD_NOT_EXISTS.toString()));
  }

  @SuppressWarnings("unused")
  private Object[] representationErrors() {
    return $($(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()));
  }

  @Test
  @Parameters(method = "representationErrors")
  public void getRepresentationReturns404IfRepresentationOrRecordDoesNotExists(Throwable exception, String errorCode)
      throws Exception {
    when(recordService.getRepresentation(globalId, schema)).thenThrow(exception);

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationPath(globalId, schema))
                                        .accept(MediaType.APPLICATION_XML))
                                    .andExpect(status().isNotFound());

    ErrorInfo errorInfo = responseContent(response, ErrorInfo.class, MediaType.APPLICATION_XML);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).getRepresentation(globalId, schema);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void deleteRecord()
      throws Exception {
    mockMvc.perform(delete(URITools.getRepresentationPath(globalId, schema)))
           .andExpect(status().isNoContent());

    verify(recordService, times(1)).deleteRepresentation(globalId, schema);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  @Parameters(method = "representationErrors")
  public void deleteRepresentationReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception,
      String errorCode)
      throws Exception {
    Mockito.doThrow(exception).when(recordService).deleteRepresentation(globalId, schema);

    ResultActions response = mockMvc.perform(delete(URITools.getRepresentationPath(globalId, schema)))
                                    .andExpect(status().isNotFound());

    ErrorInfo errorInfo = responseContentAsErrorInfo(response);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).deleteRepresentation(globalId, schema);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void createRepresentation()
      throws Exception {
    when(recordService.createRepresentation(globalId, schema, providerID, null, DATA_SET_ID, false)).thenReturn(
        new Representation(representation));

    mockMvc.perform(post(URITools.getRepresentationPath(globalId, schema))
               .contentType(MediaType.APPLICATION_FORM_URLENCODED)
               .param(ParamConstants.F_PROVIDER, providerID)
               .param(ParamConstants.DATA_SET_ID, DATA_SET_ID))
           .andExpect(status().isCreated())
           .andExpect(header().string(HttpHeaders.LOCATION,
               URITools.getVersionUri(getBaseUri(), globalId, schema, version).toString()));

    verify(recordService, times(1)).createRepresentation(globalId, schema, providerID, null, DATA_SET_ID, false);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void createRepresentationInGivenVersion()
      throws Exception {
    when(recordService.createRepresentation(globalId, schema, providerID, VERSION, DATA_SET_ID, false)).thenReturn(
        new Representation(representation));

    mockMvc.perform(post(URITools.getRepresentationPath(globalId, schema))
               .contentType(MediaType.APPLICATION_FORM_URLENCODED)
               .param(ParamConstants.F_PROVIDER, providerID)
               .param(ParamConstants.VERSION, VERSION.toString())
               .param(ParamConstants.DATA_SET_ID, DATA_SET_ID))
           .andExpect(status().isCreated())
           .andExpect(header().string(HttpHeaders.LOCATION,
               URITools.getVersionUri(getBaseUri(), globalId, schema, version).toString()));

    verify(recordService, times(1)).createRepresentation(globalId, schema, providerID, VERSION, DATA_SET_ID, false);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void createRepresentationInGivenVersionTwice()
      throws Exception {
    when(recordService.createRepresentation(globalId, schema, providerID, VERSION, DATA_SET_ID, false)).thenReturn(
        new Representation(representation));

    mockMvc.perform(post(URITools.getRepresentationPath(globalId, schema))
               .contentType(MediaType.APPLICATION_FORM_URLENCODED)
               .param(ParamConstants.F_PROVIDER, providerID)
               .param(ParamConstants.VERSION, VERSION.toString())
               .param(ParamConstants.DATA_SET_ID, DATA_SET_ID))
           .andExpect(status().isCreated())
           .andExpect(header().string(HttpHeaders.LOCATION,
               URITools.getVersionUri(getBaseUri(), globalId, schema, version).toString()));

    mockMvc.perform(post(URITools.getRepresentationPath(globalId, schema))
               .contentType(MediaType.APPLICATION_FORM_URLENCODED)
               .param(ParamConstants.F_PROVIDER, providerID)
               .param(ParamConstants.VERSION, VERSION.toString())
               .param(ParamConstants.DATA_SET_ID, DATA_SET_ID))
           .andExpect(status().isCreated())
           .andExpect(header().string(HttpHeaders.LOCATION,
               URITools.getVersionUri(getBaseUri(), globalId, schema, version).toString()));

    verify(recordService, times(2)).createRepresentation(globalId, schema, providerID, VERSION, DATA_SET_ID, false);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  @Parameters(method = "recordErrors")
  public void createRepresentationReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception,
      String errorCode)
      throws Exception {
    Mockito.doThrow(exception).when(recordService).createRepresentation(globalId, schema, providerID, null, DATA_SET_ID, false);

    ResultActions response = mockMvc.perform(post(URITools.getRepresentationPath(globalId, schema))
                                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                                        .param(ParamConstants.F_PROVIDER, providerID)
                                        .param(ParamConstants.DATA_SET_ID, DATA_SET_ID))
                                    .andExpect(status().isNotFound());

    ErrorInfo errorInfo = responseContentAsErrorInfo(response);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).createRepresentation(globalId, schema, providerID, null, DATA_SET_ID, false);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void createRepresentationReturns404IfProviderIdIsNotGiven()
      throws Exception {
    ResultActions response = mockMvc.perform(post(URITools.getRepresentationPath(globalId, schema))
                                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                                    .andExpect(status().isBadRequest());

    ErrorInfo errorInfo = responseContentAsErrorInfo(response);
    assertThat(errorInfo.getErrorCode(), is(McsErrorCode.OTHER.toString()));
  }
}
