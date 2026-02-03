package eu.europeana.cloud.service.mcs.controller;

import com.google.common.collect.Lists;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.ResultActions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
class RepresentationsResourceTest extends AbstractResourceTest {

    private RecordService recordService;

    static final private String globalId = "1";
    static final private String schema = "DC";
    static final private String version = "1.0";
    static final private Record record = new Record(globalId, Lists.newArrayList(new Representation(globalId, schema,
            version, null, null, "DLF", Arrays.asList(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
            "2013-01-01", 12345, null)), null, true, new Date(), null)));


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
    void getRepresentations(MediaType mediaType) throws Exception {
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
    void getRepresentationsReturns404IfRecordDoesNotExists()
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
    void getRepresentationsReturns406ForUnsupportedFormat() throws Exception {
        mockMvc.perform(get(URITools.getRepresentationsPath(globalId))
                        .accept(MEDIA_TYPE_APPLICATION_SVG_XML))
                .andExpect(status().isNotAcceptable());
    }

}
