package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.RestInterfaceConstants;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import jakarta.ws.rs.core.HttpHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(SpringExtension.class)
public class RepresentationVersionResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  private static final String GLOBAL_ID = "1";
  private static final String SCHEMA = "DC";
  private static final String VERSION = "1.0";
  private static final String FILE_NAME = "1.xml";
  private static final String PERSIST_PATH =
      UriComponentsBuilder.fromUriString(RestInterfaceConstants.REPRESENTATION_VERSION_PERSIST)
                          .build(GLOBAL_ID, SCHEMA, VERSION).toString();


  private static final Representation REPRESENTATION = new Representation(GLOBAL_ID, SCHEMA, VERSION, null, null,
      "DLF", List.of(new File(FILE_NAME, "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01",
          12345, null)), null, true, new Date(), null);


  @BeforeEach
  void mockUp() throws RepresentationNotExistsException {
    recordService = applicationContext.getBean(RecordService.class);
    CassandraRecordDAO cassandraRecordDAO = applicationContext.getBean(CassandraRecordDAO.class);
    DataSetPermissionsVerifier dataSetPermissionsVerifier = applicationContext.getBean(DataSetPermissionsVerifier.class);
    Mockito.reset(recordService);

    when(cassandraRecordDAO.getRepresentationDatasetId(any(), any()))
            .thenReturn(Optional.of(new CompoundDataSetId("dsProvId", "datasetId")));

    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToPersistRepresentation(any());
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToDelete(any());

  }


  static MediaType[] mimeTypes() {
    return new MediaType[]{MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON};
  }

  @ParameterizedTest
  @MethodSource("mimeTypes")
  void testGetRepresentationVersion(MediaType mediaType) throws Exception {
    Representation expected = new Representation(REPRESENTATION);
    URITools.enrich(expected, getBaseUri());
    when(recordService.getRepresentation(GLOBAL_ID, SCHEMA, VERSION)).thenReturn(new Representation(REPRESENTATION));

    ResultActions response = mockMvc.perform(get(URITools.getVersionPath(GLOBAL_ID, SCHEMA, VERSION)).accept(mediaType))
            .andExpect(status().isOk())
            .andExpect(content().contentType(mediaType));

    Representation entity = responseContent(response, Representation.class, mediaType);
    assertThat(entity, is(expected));
    verify(recordService, times(1)).getRepresentation(GLOBAL_ID, SCHEMA, VERSION);
    verifyNoMoreInteractions(recordService);
  }


  private static Stream<Arguments> errors() {
    return Stream.of(
            arguments(
                    new RepresentationNotExistsException(),
                    McsErrorCode.REPRESENTATION_NOT_EXISTS.toString(),
                    404
            )
    );
  }


  @ParameterizedTest
  @MethodSource("errors")
  void testGetRepresentationVersionReturns404IfRepresentationOrRecordOrVersionDoesNotExists(
          Throwable exception, String errorCode, int statusCode)
          throws Exception {
    when(recordService.getRepresentation(GLOBAL_ID, SCHEMA, VERSION)).thenThrow(exception);

    ResultActions response = mockMvc.perform(get(URITools.getVersionPath(GLOBAL_ID, SCHEMA, VERSION))
                    .accept(MediaType.APPLICATION_XML))
            .andExpect(status().is(statusCode));

    ErrorInfo errorInfo = responseContentAsErrorInfo(response, MediaType.APPLICATION_XML);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).getRepresentation(GLOBAL_ID, SCHEMA, VERSION);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  void testGetRepresentationVersionReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get(URITools.getVersionPath(GLOBAL_ID, SCHEMA, VERSION))
                    .accept(MEDIA_TYPE_APPLICATION_SVG_XML))
            .andExpect(status().isNotAcceptable());
  }


  @Test
  void testDeleteRepresentation()
          throws Exception {
    mockMvc.perform(delete(URITools.getVersionPath(GLOBAL_ID, SCHEMA, VERSION)))
            .andExpect(status().isNoContent());
    verify(recordService, times(1)).deleteRepresentation(GLOBAL_ID, SCHEMA, VERSION);
    verifyNoMoreInteractions(recordService);
  }


  @ParameterizedTest
  @MethodSource("errors")
  void testDeleteRepresentationReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception,
                                                                               String errorCode, int statusCode)
          throws Exception {
    Mockito.doThrow(exception).when(recordService).deleteRepresentation(GLOBAL_ID, SCHEMA, VERSION);

    ResultActions response = mockMvc.perform(delete(URITools.getVersionPath(GLOBAL_ID, SCHEMA, VERSION)))
            .andExpect(status().is(statusCode));

    ErrorInfo errorInfo = responseContentAsErrorInfo(response);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).deleteRepresentation(GLOBAL_ID, SCHEMA, VERSION);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  void testPersistRepresentation()
          throws Exception {
    when(recordService.persistRepresentation(GLOBAL_ID, SCHEMA, VERSION)).thenReturn(
            new Representation(REPRESENTATION));

    mockMvc.perform(post(PERSIST_PATH).contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().isCreated())
            .andExpect(
                    header().string(HttpHeaders.LOCATION, URITools.getVersionUri(getBaseUri(), GLOBAL_ID, SCHEMA, VERSION).toString()));

    verify(recordService, times(1)).persistRepresentation(GLOBAL_ID, SCHEMA, VERSION);
    verifyNoMoreInteractions(recordService);
  }


  @ParameterizedTest
  @MethodSource("persistErrors")
  void testPersistRepresentationReturns40XIfExceptionOccur(Throwable exception, String errorCode,
                                                           int statusCode)
          throws Exception {
    when(recordService.persistRepresentation(GLOBAL_ID, SCHEMA, VERSION)).thenThrow(exception);

    ResultActions response = mockMvc.perform(post(PERSIST_PATH).contentType(MediaType.APPLICATION_FORM_URLENCODED))
            .andExpect(status().is(statusCode));

    ErrorInfo errorInfo = responseContentAsErrorInfo(response);
    assertThat(errorInfo.getErrorCode(), is(errorCode));
    verify(recordService, times(1)).persistRepresentation(GLOBAL_ID, SCHEMA, VERSION);
    verifyNoMoreInteractions(recordService);
  }


  private static Stream<Arguments> persistErrors() {
    return Stream.of(
            arguments(new RepresentationNotExistsException(),
                    McsErrorCode.REPRESENTATION_NOT_EXISTS.toString(),
                    404),

            arguments(new CannotModifyPersistentRepresentationException(),
                    McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION.toString(),
                    405),

            arguments(new CannotPersistEmptyRepresentationException(),
                    McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION.toString(),
                    405)
    );
  }
}
