package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.util.Collections;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.test.web.servlet.ResultActions;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.MEDIA_TYPE_APPLICATION_SVG_XML;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsRepresentationList;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsRepresentationRevisionResponseList;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(JUnitParamsRunner.class)
public class RepresentationRevisionsResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  private static final String GLOBAL_ID = "1";
  private static final String SCHEMA = "DC";
  private static final String REVISION_PROVIDER_ID = "ABC";
  private static final String REVISION_NAME = "rev1";
  private static final String VERSION = "1.0";
  private static final String REPRESENTATION_NAME = "rep1";
  private static final Date REVISION_TIMESTAMP = new Date();
  private static final RepresentationRevisionResponse representationResponse = new RepresentationRevisionResponse(GLOBAL_ID,
          SCHEMA, VERSION, List.of(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
      null)), REVISION_PROVIDER_ID, REVISION_NAME, REVISION_TIMESTAMP);


  @Before
  public void mockUp() {
    recordService = applicationContext.getBean(RecordService.class);
    Mockito.reset(recordService);
    //
    AclPermissionEvaluator permissionEvaluator = applicationContext.getBean(AclPermissionEvaluator.class);
    Mockito.when(
               permissionEvaluator.hasPermission(
                   Mockito.any(Authentication.class),
                   Mockito.any(Serializable.class),
                   Mockito.any(String.class),
                   Mockito.anyObject()))
           .thenReturn(true);
  }
  @SuppressWarnings("unused")
  private Object[] mimeTypes() {
    return $($(MediaType.APPLICATION_XML), $(MediaType.APPLICATION_JSON));
  }

  @Test
  @Parameters(method = "mimeTypes")
  public void getRepresentationByRevisionResponse(MediaType mediaType)
      throws Exception {
    RepresentationRevisionResponse representationRevisionResponse = new RepresentationRevisionResponse(representationResponse);
    ArrayList<File> files = new ArrayList<>(1);
    files.add(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
        "2013-01-01", 12345L, URI.create("http://localhost:80/records/" + GLOBAL_ID
        + "/representations/" + SCHEMA + "/versions/" + VERSION + "/files/1.xml")));
    representationRevisionResponse.setFiles(files);

    Representation representation = new Representation(representationRevisionResponse.getCloudId(),
        representationRevisionResponse.getRepresentationName(), representationRevisionResponse.getVersion(),
        null, null, representationRevisionResponse.getRevisionProviderId(), representationRevisionResponse.getFiles(),
        new ArrayList<Revision>(), false, representationRevisionResponse.getRevisionTimestamp(), null, false);

    List<RepresentationRevisionResponse> expectedResponse = new ArrayList<>();
    expectedResponse.add(representationRevisionResponse);

    doReturn(expectedResponse).when(recordService).getRepresentationRevisions(GLOBAL_ID,
            SCHEMA, REVISION_PROVIDER_ID, REVISION_NAME, null);

    when(recordService.getRepresentation(GLOBAL_ID, representationResponse.getRepresentationName(),
        representationResponse.getVersion())).thenReturn(representation);

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationRevisionsPath(GLOBAL_ID, SCHEMA, REVISION_NAME))
                                        .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, REVISION_PROVIDER_ID).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    List<Representation> entity = responseContentAsRepresentationList(response, mediaType);
    assertThat(entity.size(), is(1));
    assertThat(entity.get(0), is(representation));
    verify(recordService, times(1)).getRepresentationRevisions(GLOBAL_ID, SCHEMA, REVISION_PROVIDER_ID, REVISION_NAME, null);
    verify(recordService, times(1)).getRepresentation(GLOBAL_ID, SCHEMA, representationRevisionResponse.getVersion());
    verifyNoMoreInteractions(recordService);
  }

  @Test
  @Parameters(method = "mimeTypes")
  public void getgetRepresentationRawRevisionsResponse(MediaType mediaType)
      throws Exception {
    RepresentationRevisionResponse representationRevisionResponse = new RepresentationRevisionResponse(representationResponse);
    representationRevisionResponse.setFiles(Collections.singletonList(
        new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
            "2013-01-01", 12345L, URI.create("http://localhost:80/records/" + GLOBAL_ID
            + "/representations/" + SCHEMA + "/versions/" + VERSION + "/files/1.xml"))));
    doReturn(Collections.singletonList(representationRevisionResponse)).when(recordService).getRepresentationRevisions(GLOBAL_ID,
        SCHEMA, REVISION_PROVIDER_ID, REVISION_NAME, null);

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationRawRevisionsPath(GLOBAL_ID, SCHEMA, REVISION_NAME))
                                        .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, REVISION_PROVIDER_ID).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    List<RepresentationRevisionResponse> entity = responseContentAsRepresentationRevisionResponseList(response, mediaType);
    assertThat(entity.size(), is(1));
    assertThat(entity.getFirst(), is(representationRevisionResponse));
    verify(recordService, times(1)).getRepresentationRevisions(GLOBAL_ID, SCHEMA, REVISION_PROVIDER_ID, REVISION_NAME, null);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void getRepresentationReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get(URITools.getRepresentationRevisionsPath(GLOBAL_ID, SCHEMA, REVISION_NAME))
        .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, REVISION_PROVIDER_ID)
        .accept(MEDIA_TYPE_APPLICATION_SVG_XML)).andExpect(status().isNotAcceptable());
  }


  @Test
  public void getRepresentationByRevisionsThrowExceptionWhenReturnsEmptyObjectIfRevisionDoesNotExists()
      throws Exception {
    List<RepresentationRevisionResponse> expectedResponse = new ArrayList<>();
    RepresentationRevisionResponse response = mock(RepresentationRevisionResponse.class);
    when(response.getRepresentationName()).thenReturn(REPRESENTATION_NAME);
    when(response.getCloudId()).thenReturn(GLOBAL_ID);
    when(response.getVersion()).thenReturn(VERSION);
    expectedResponse.add(response);
    doReturn(expectedResponse).when(recordService).getRepresentationRevisions(GLOBAL_ID,
            SCHEMA, REVISION_PROVIDER_ID, REVISION_NAME, null);
    doThrow(RepresentationNotExistsException.class).when(recordService).getRepresentation(anyString(), anyString(), anyString());

    mockMvc.perform(get(URITools.getRepresentationRevisionsPath(GLOBAL_ID, SCHEMA, REVISION_NAME))
               .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, REVISION_PROVIDER_ID)
               .accept(MediaType.APPLICATION_XML))
           .andExpect(status().isNotFound());

    verify(recordService, times(1)).getRepresentationRevisions(GLOBAL_ID, SCHEMA, REVISION_PROVIDER_ID, REVISION_NAME, null);
    verify(recordService, times(1)).getRepresentation(anyString(), anyString(), anyString());
  }

}

