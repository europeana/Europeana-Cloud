package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.MEDIA_TYPE_APPLICATION_SVG_XML;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsRepresentationList;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.io.Serializable;
import java.net.URI;
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
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.test.web.servlet.ResultActions;

@RunWith(JUnitParamsRunner.class)
public class RepresentationRevisionsResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  static final private String globalId = "1";
  static final private String schema = "DC";
  static final private String revisionProviderId = "ABC";
  static final private String revisionName = "rev1";
  static final private String version = "1.0";
  static final private String representationName = "rep1";
  static final private Date revisionTimestamp = new Date();
  static final private RepresentationRevisionResponse representationResponse = new RepresentationRevisionResponse(globalId,
      schema, version, List.of(new File("1.xml", "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
      null)), revisionProviderId, revisionName, revisionTimestamp);

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
        "2013-01-01", 12345L, URI.create("http://localhost:80/records/" + globalId
        + "/representations/" + schema + "/versions/" + version + "/files/1.xml")));
    representationRevisionResponse.setFiles(files);

    Representation representation = new Representation(representationRevisionResponse.getCloudId(),
        representationRevisionResponse.getRepresentationName(), representationRevisionResponse.getVersion(),
        null, null, representationRevisionResponse.getRevisionProviderId(), representationRevisionResponse.getFiles(),
        new ArrayList<Revision>(), false, representationRevisionResponse.getRevisionTimestamp());

    List<RepresentationRevisionResponse> expectedResponse = new ArrayList<>();
    expectedResponse.add(representationRevisionResponse);

    doReturn(expectedResponse).when(recordService).getRepresentationRevisions(globalId,
        schema, revisionProviderId, revisionName, null);

    when(recordService.getRepresentation(globalId, representationResponse.getRepresentationName(),
        representationResponse.getVersion())).thenReturn(representation);

    ResultActions response = mockMvc.perform(get(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName))
                                        .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    List<Representation> entity = responseContentAsRepresentationList(response, mediaType);
    assertThat(entity.size(), is(1));
    assertThat(entity.get(0), is(representation));
    verify(recordService, times(1)).getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null);
    verify(recordService, times(1)).getRepresentation(globalId, schema, representationRevisionResponse.getVersion());
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void getRepresentationReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName))
        .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId)
        .accept(MEDIA_TYPE_APPLICATION_SVG_XML)).andExpect(status().isNotAcceptable());
  }


  @Test
  public void getRepresentationByRevisionsThrowExceptionWhenReturnsEmptyObjectIfRevisionDoesNotExists()
      throws Exception {
    List<RepresentationRevisionResponse> expectedResponse = new ArrayList<>();
    RepresentationRevisionResponse response = mock(RepresentationRevisionResponse.class);
    when(response.getRepresentationName()).thenReturn(representationName);
    when(response.getCloudId()).thenReturn(globalId);
    when(response.getVersion()).thenReturn(version);
    expectedResponse.add(response);
    doReturn(expectedResponse).when(recordService).getRepresentationRevisions(globalId,
        schema, revisionProviderId, revisionName, null);
    doThrow(RepresentationNotExistsException.class).when(recordService).getRepresentation(anyString(), anyString(), anyString());

    mockMvc.perform(get(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName))
               .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId)
               .accept(MediaType.APPLICATION_XML))
           .andExpect(status().isNotFound());

    verify(recordService, times(1)).getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null);
    verify(recordService, times(1)).getRepresentation(anyString(), anyString(), anyString());
  }

  @Test
  public void getRepresentationByRevisionsThrowExceptionWhenReturnsRepresentationRevisionResponseIsNull()
      throws Exception {
    when(recordService.getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null)).thenReturn(null);
    mockMvc.perform(get(URITools.getRepresentationRevisionsPath(globalId, schema, revisionName))
               .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId)
               .accept(MediaType.APPLICATION_XML))
           .andExpect(status().isNotFound());

    verify(recordService, times(1)).getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, null);
  }
}

