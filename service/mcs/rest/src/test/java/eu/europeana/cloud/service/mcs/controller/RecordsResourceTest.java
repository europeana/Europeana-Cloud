package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.MEDIA_TYPE_APPLICATION_SVG_XML;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.getBaseUri;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContent;
import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.common.collect.Lists;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.net.URI;
import java.util.Date;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

@RunWith(JUnitParamsRunner.class)
public class RecordsResourceTest extends AbstractResourceTest {

  private RecordService recordService;

  @Before
  public void mockUp() {
    //        uisClient = applicationContext.getBean(null)
    recordService = applicationContext.getBean(RecordService.class);
    Mockito.reset(recordService);
  }

  @Test
  @Parameters(method = "mimeTypes")
  public void getRecord(MediaType mediaType)
      throws Exception {
    String globalId = "global1";
    Record record = new Record(globalId, Lists.newArrayList(new Representation(globalId, "DC", "1", null, null,
        "FBC", Lists.newArrayList(new File("dc.xml", "text/xml", "91162629d258a876ee994e9233b2ad87",
        "2013-10-21", 12345L, URI.create("http://localhost/records/" + globalId
        + "/representations/DC/versions/1/files/dc.xml"))), null, false, new Date())));
    Record expected = new Record(record);
    Representation expectedRepresentation = expected.getRepresentations().get(0);
    // prepare expected representation:
    // - erase record id
    // - set URIs
    expectedRepresentation.setCloudId(null);
    expectedRepresentation.setAllVersionsUri(URI.create(getBaseUri() + "records/" + globalId
        + "/representations/DC/versions"));
    expectedRepresentation.setUri(URI.create(getBaseUri() + "records/" + globalId
        + "/representations/DC/versions/1"));
    when(recordService.getRecord(globalId)).thenReturn(record);

    ResultActions response = mockMvc.perform(get("/records/" + globalId).accept(mediaType))
                                    .andExpect(status().isOk())
                                    .andExpect(content().contentType(mediaType));

    System.out.println("Response body:\n" + response.andReturn().getResponse().getContentAsString());
    Record entity = responseContent(response, Record.class, mediaType);
    assertThat(entity, is(expected));
    verify(recordService, times(1)).getRecord(globalId);
    verifyNoMoreInteractions(recordService);
  }

  @Test
  public void getRecordReturns406ForUnsupportedFormat() throws Exception {
    mockMvc.perform(get("/records/global1").accept(MEDIA_TYPE_APPLICATION_SVG_XML))
           .andExpect(status().isNotAcceptable());
  }


  @SuppressWarnings("unused")
  private Object[] mimeTypes() {
    return $($(MediaType.APPLICATION_XML), $(MediaType.APPLICATION_JSON));
  }


  @Test
  public void getRecordReturns404IfRecordDoesNotExists()
      throws Exception {
    String globalId = "global1";
    Throwable exception = new RecordNotExistsException();
    when(recordService.getRecord(globalId)).thenThrow(exception);

    mockMvc.perform(get("/records/" + globalId).contentType(MediaType.APPLICATION_XML))
           .andExpect(status().isNotFound());

    verify(recordService, times(1)).getRecord(globalId);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void deleteRecord()
      throws Exception {
    String globalId = "global1";

    mockMvc.perform(delete("/records/" + globalId))
           .andExpect(status().isNoContent());

    verify(recordService, times(1)).deleteRecord(globalId);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void deleteRecordReturns404IfRecordDoesNotExists()
      throws Exception {
    String globalId = "global1";
    Throwable exception = new RecordNotExistsException();
    Mockito.doThrow(exception).when(recordService).deleteRecord(globalId);

    mockMvc.perform(delete("/records/" + globalId).contentType(MediaType.APPLICATION_XML))
           .andExpect(status().isNotFound());

    verify(recordService, times(1)).deleteRecord(globalId);
    verifyNoMoreInteractions(recordService);
  }


  @Test
  public void deleteRecordReturns404IfRecordHasNoRepresentations() throws Exception {
    String globalId = "global1";
    Throwable exception = new RepresentationNotExistsException();
    Mockito.doThrow(exception).when(recordService).deleteRecord(globalId);

    mockMvc.perform(delete("/records/" + globalId).contentType(MediaType.APPLICATION_XML))
           .andExpect(status().isNotFound());

    verify(recordService, times(1)).deleteRecord(globalId);
    verifyNoMoreInteractions(recordService);
  }


}
