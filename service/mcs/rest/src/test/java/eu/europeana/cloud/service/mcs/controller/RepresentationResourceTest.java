package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.ResultActions;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

import static eu.europeana.cloud.common.web.ParamConstants.DATA_SET_ID;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(SpringExtension.class)
class RepresentationResourceTest extends AbstractResourceTest {

    private static final UUID VERSION = UUID.fromString(new com.eaio.uuid.UUID().toString());

    private RecordService recordService;

    static final private String globalId = "1";
    static final private String schema = "DC";
    static final private String version = "1.0";
    static final private String providerID = "DLF";
    static final private Representation representation = new Representation(globalId, schema, version, null, null,
            "DLF", Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
            null)), true, new Date(), emptySet(), false);

    @BeforeEach
    void mockUp() {
        recordService = applicationContext.getBean(RecordService.class);
        Mockito.reset(recordService);
    }

    private static MediaType[] mimeTypes() {
        return new MediaType[]{MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON};
    }

    @ParameterizedTest
    @MethodSource("mimeTypes")
    void getRepresentation(MediaType mediaType)
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
    void getRepresentationReturns406ForUnsupportedFormat() throws Exception {
        mockMvc.perform(get(URITools.getRepresentationPath(globalId, schema))
                        .accept(MEDIA_TYPE_APPLICATION_SVG_XML))
                .andExpect(status().isNotAcceptable());
    }

    private static Stream<Arguments> recordErrors() {
        return Stream.of(
                arguments(new RecordNotExistsException(), McsErrorCode.RECORD_NOT_EXISTS.toString())
        );
    }

    private static Stream<Arguments> representationErrors() {
        return Stream.of(
                arguments(new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString())
        );
    }

    @ParameterizedTest
    @MethodSource("representationErrors")
    void getRepresentationReturns404IfRepresentationOrRecordDoesNotExists(Throwable exception, String errorCode)
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
    void deleteRecord()
            throws Exception {
        mockMvc.perform(delete(URITools.getRepresentationPath(globalId, schema)))
                .andExpect(status().isNoContent());

        verify(recordService, times(1)).deleteRepresentation(globalId, schema);
        verifyNoMoreInteractions(recordService);
    }

    @ParameterizedTest
    @MethodSource("representationErrors")
    void deleteRepresentationReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception,
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
    void createRepresentation()
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
    void createRepresentationInGivenVersion()
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
    void createRepresentationInGivenVersionTwice()
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

    @ParameterizedTest
    @MethodSource("recordErrors")
    void createRepresentationReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception,
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
    void createRepresentationReturns404IfProviderIdIsNotGiven()
            throws Exception {
        ResultActions response = mockMvc.perform(post(URITools.getRepresentationPath(globalId, schema))
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isBadRequest());

        ErrorInfo errorInfo = responseContentAsErrorInfo(response);
        assertThat(errorInfo.getErrorCode(), is(McsErrorCode.OTHER.toString()));
    }
}
