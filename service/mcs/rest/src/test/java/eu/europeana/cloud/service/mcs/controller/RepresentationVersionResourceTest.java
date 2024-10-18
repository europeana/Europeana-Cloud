package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.RestInterfaceConstants;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import jakarta.ws.rs.core.HttpHeaders;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(JUnitParamsRunner.class)
public class RepresentationVersionResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  static final private String globalId = "1";
  static final private String schema = "DC";
  static final private String version = "1.0";
  static final private String fileName = "1.xml";
  static final private String persistPath =
      UriComponentsBuilder.fromUriString(RestInterfaceConstants.REPRESENTATION_VERSION_PERSIST)
                          .build(globalId, schema, version).toString();

  static final private String copyPath =
      UriComponentsBuilder.fromUriString(RestInterfaceConstants.REPRESENTATION_VERSION_COPY)
                          .build(globalId, schema, version).toString();


  static final private Representation representation = new Representation(globalId, schema, version, null, null,
      "DLF", Arrays.asList(new File(fileName, "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01",
      12345, null)), null, true, new Date());


  @Before
  public void mockUp() throws RepresentationNotExistsException, DataSetAssignmentException {
    recordService = applicationContext.getBean(RecordService.class);
    CassandraRecordDAO cassandraRecordDAO = applicationContext.getBean(CassandraRecordDAO.class);
    DataSetPermissionsVerifier dataSetPermissionsVerifier = applicationContext.getBean(DataSetPermissionsVerifier.class);
    Mockito.reset(recordService);

    when(cassandraRecordDAO.getRepresentationDatasetId(any()))
        .thenReturn(Optional.of(new CompoundDataSetId("dsProvId", "datasetId")));

    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToPersistRepresentation(any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToDelete(any());

  }


  @SuppressWarnings("unused")
  private Object[] mimeTypes() {
    return $($(MediaType.APPLICATION_XML), $(MediaType.APPLICATION_JSON));
  }


  @Test
  @Parameters(method = "mimeTypes")
  public void testGetRepresentationVersion(MediaType mediaType) throws Exception {
    Representation expected = new Representation(representation);
    URITools.enrich(expected, getBaseUri());
    when(recordService.getRepresentation(globalId, schema, version)).thenReturn(new Representation(representation));

    ResultActions response = mockMvc.perform(get(URITools.getVersionPath(globalId, schema, version)).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    Representation entity = responseContent(response, Representation.class, mediaType);
    assertThat(entity, is(expected));
    verify(recordService, times(1)).getRepresentation(globalId, schema, version);
    verifyNoMoreInteractions(recordService);
  }

  private Object[] errors() {
    return $($(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString(), 404));
  }


  @Test
  @Parameters(method = "errors")
  public void testGetRepresentationVersionReturns404IfRepresentationOrRecordOrVersionDoesNotExists(
      Throwable exception, String errorCode, int statusCode)
      throws Exception {
    when(recordService.getRepresentation(globalId, schema, version)).thenThrow(exception);

    ResultActions response = mockMvc.perform(get(URITools.getVersionPath(globalId, schema, version))
                                        .accept(MediaType.APPLICATION_XML))
                                    .andExpect(status().is(statusCode));

    ErrorInfo errorInfo = responseContentAsErrorInfo(response, MediaType.APPLICATION_XML);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).getRepresentation(globalId, schema, version);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void testGetRepresentationVersionReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get(URITools.getVersionPath(globalId, schema, version))
               .accept(MEDIA_TYPE_APPLICATION_SVG_XML))
           .andExpect(status().isNotAcceptable());
  }


  @Test
  public void testDeleteRepresentation()
      throws Exception {
    mockMvc.perform(delete(URITools.getVersionPath(globalId, schema, version)))
           .andExpect(status().isNoContent());
    verify(recordService, times(1)).deleteRepresentation(globalId, schema, version);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  @Parameters(method = "errors")
  public void testDeleteRepresentationReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception,
      String errorCode, int statusCode)
      throws Exception {
    Mockito.doThrow(exception).when(recordService).deleteRepresentation(globalId, schema, version);

    ResultActions response = mockMvc.perform(delete(URITools.getVersionPath(globalId, schema, version)))
                                    .andExpect(status().is(statusCode));

    ErrorInfo errorInfo = responseContentAsErrorInfo(response);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).deleteRepresentation(globalId, schema, version);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void testPersistRepresentation()
      throws Exception {
    when(recordService.persistRepresentation(globalId, schema, version)).thenReturn(
        new Representation(representation));

    mockMvc.perform(post(persistPath).contentType(MediaType.APPLICATION_FORM_URLENCODED))
           .andExpect(status().isCreated())
           .andExpect(
               header().string(HttpHeaders.LOCATION, URITools.getVersionUri(getBaseUri(), globalId, schema, version).toString()));

    verify(recordService, times(1)).persistRepresentation(globalId, schema, version);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  @Parameters(method = "persistErrors")
  public void testPersistRepresentationReturns40XIfExceptionOccur(Throwable exception, String errorCode,
      int statusCode)
      throws Exception {
    when(recordService.persistRepresentation(globalId, schema, version)).thenThrow(exception);

    ResultActions response = mockMvc.perform(post(persistPath).contentType(MediaType.APPLICATION_FORM_URLENCODED))
                                    .andExpect(status().is(statusCode));

    ErrorInfo errorInfo = responseContentAsErrorInfo(response);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).persistRepresentation(globalId, schema, version);
    verifyNoMoreInteractions(recordService);
  }


  @SuppressWarnings("unused")
  private Object[] persistErrors() {
    return $(
        $(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString(), 404),
        $(new CannotModifyPersistentRepresentationException(),
            McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION.toString(), 405),
        $(new CannotPersistEmptyRepresentationException(),
            McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION.toString(), 405));
  }

}
