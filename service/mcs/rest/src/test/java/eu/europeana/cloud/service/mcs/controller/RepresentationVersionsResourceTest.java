package eu.europeana.cloud.service.mcs.controller;

import com.google.common.collect.ImmutableList;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
public class RepresentationVersionsResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  static final private String GLOBAL_ID = "1";
  static final private String SCHEMA = "DC";
  static final private String VERSION = "1.0";

  private static final String LIST_VERSIONS_PATH = URITools.getListVersionsPath(GLOBAL_ID, SCHEMA).toString();
  static final private List<Representation> REPRESENTATIONS = ImmutableList.of(new Representation(GLOBAL_ID, SCHEMA,
      VERSION, null, null, "DLF", Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
          "2013-01-01", 12345, null)), null, true, new Date(), null));


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
    void testListVersions(MediaType mediaType)
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


    private static Stream<Arguments> errors() {
        return Stream.of(
                arguments(
                        new RepresentationNotExistsException(),
                        McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()
                )
        );
    }

    @ParameterizedTest
    @MethodSource("errors")
    void testListVersionsReturns404IfRecordOrRepresentationDoesNotExists(Throwable exception, String errorCode)
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
    void testListVersionsReturns406ForUnsupportedFormat() throws Exception {
        mockMvc.perform(get(LIST_VERSIONS_PATH).accept(MEDIA_TYPE_APPLICATION_SVG_XML))
                .andExpect(status().isNotAcceptable());
    }

}
