package eu.europeana.cloud.service.mcs.controller;


import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsCloudTagResultSlice;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


/**
 * DataSetResourceTest
 *
 * @author akrystian
 */
@RunWith(CassandraTestRunner.class)
public class DataSetRevisionsResourceTest extends CassandraBasedAbstractResourceTest {

  private DataSetService dataSetService;

  private String dataSetWebTarget;

  private UISClientHandler uisHandler;

  private SimpleDateFormat dateFormat;

  private static final String VERSION_ID = "b75fe730-f893-11ed-9c21-0242da5f01d7";

  @Before
  public void mockUp() {
    uisHandler = applicationContext.getBean(UISClientHandler.class);
    dataSetService = applicationContext.getBean(DataSetService.class);
    dataSetWebTarget = DataSetRevisionsResource.class.getAnnotation(RequestMapping.class).value()[0];
    dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
  }

  @After
  public void cleanUp() {
    Mockito.reset(uisHandler);
  }

  @Test
  public void shouldRetrieveCloudIdBelongToRevision() throws Exception {
    // given
    String datasetId = "dataset";
    String providerId = "providerId";
    String revisionName = "revisionName";
    String revisionProviderId = "revisionProviderId";
    String representationName = "representationName";
    String cloudId = "cloudId";
    Revision revision = new Revision(revisionName, revisionProviderId, new Date(), false);
    Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
    Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
    dataSetService.createDataSet(providerId, datasetId, "");
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId, VERSION_ID);

    // when
    ResultActions response = mockMvc.perform(
                                        get(dataSetWebTarget, providerId, datasetId, representationName, revisionName, revisionProviderId)
                                            .queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp()))
                                            .queryParam(F_LIMIT, "10"))
                                    .andDo(print())
                                    .andExpect(status().isOk());

    //then
    List<CloudTagsResponse> cloudIds = responseContentAsCloudTagResultSlice(response).getResults();
    assertThat(cloudIds.size(), is(1));
    assertThat(cloudIds.get(0).getCloudId(), is(cloudId));
  }

  @Test
  public void shouldGetEmptyResultSetOnRetrievingNonExistingRevision() throws Exception {
    // given
    String datasetId = "dataset";
    String providerId = "providerId";
    String revisionName = "revisionName";
    String revisionName2 = "revisionName2";
    String revisionProviderId = "revisionProviderId";
    String representationName = "representationName";
    Revision revision = new Revision(revisionName, revisionProviderId, new Date(), false);
    Revision revision2 = new Revision(revisionName2, revisionProviderId, new Date(), false);
    String cloudId = "cloudId";
    Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
    Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
    dataSetService.createDataSet(providerId, datasetId, "");
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId, VERSION_ID);

    // when
    ResultActions response = mockMvc.perform(
                                        get(dataSetWebTarget, providerId, datasetId, representationName, revisionName2, revisionProviderId).
                                            queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision2.getCreationTimeStamp())).
                                            queryParam(F_LIMIT, "1"))
                                    .andExpect(status().isOk());

    //then
    assertThat(responseContentAsCloudTagResultSlice(response).getResults().size(), is(0));
  }

  @Test
  public void shouldGetEmptyResultSetOnRetrievingNonExistingRevision2() throws Exception {
    // given
    String datasetId = "dataset";
    String providerId = "providerId";
    String revisionName = "revisionName";
    String revisionProviderId = "revisionProviderId";
    String representationName = "representationName";
    String revisionTimestamp = "2016-12-02T01:08:28.059";
    Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
    Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
    dataSetService.createDataSet(providerId, datasetId, "");

    // when
    ResultActions response = mockMvc.perform(
                                        get(dataSetWebTarget, providerId, datasetId, representationName, revisionName, revisionProviderId)
                                            .queryParam(F_REVISION_TIMESTAMP, revisionTimestamp)
                                            .queryParam(F_LIMIT, "10"))
                                    .andExpect(status().isOk());

    //then
    assertThat(responseContentAsCloudTagResultSlice(response).getResults().size(), is(0));

  }


  @Test
  public void shouldResultWithNotDefinedStart() throws Exception {
    // given
    String datasetId = "dataset";
    String providerId = "providerId";
    String representationName = "representationName";
    String revisionName = "revisionName";
    String revisionProviderId = "revisionProviderId";
    Revision revision = new Revision(revisionName, revisionProviderId, new Date(), false);
    String cloudId = "cloudId";
    String cloudId2 = "cloudId2";
    String cloudId3 = "cloudId3";
    Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
    Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
    dataSetService.createDataSet(providerId, datasetId, "");
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId, VERSION_ID);
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId2, VERSION_ID);
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId3, VERSION_ID);
    // when
    ResultActions response = mockMvc.perform(
                                        get(dataSetWebTarget, providerId, datasetId, representationName, revisionName, revisionProviderId).
                                            queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp())).
                                            queryParam(F_LIMIT, "1"))
                                    .andExpect(status().isOk());

    //then
    List<CloudTagsResponse> cloudIds = responseContentAsCloudTagResultSlice(response).getResults();
    assertThat(cloudIds.size(), is(1));
    assertThat(cloudIds.get(0).getCloudId(), is(cloudId));
  }

  @Test
  public void shouldRetrieveCloudIdsPageByPage() throws Exception {
    // given
    String datasetId = "dataset";
    String providerId = "providerId";
    String representationName = "representationName";
    String revisionName = "revisionName";
    String revisionProviderId = "revisionProviderId";

    Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
    Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
    dataSetService.createDataSet(providerId, datasetId, "");

    Revision revision = new Revision(revisionName, revisionProviderId, new Date(), false);
    String cloudId = "cloudId";
    String cloudId2 = "cloudId2";
    String cloudId3 = "cloudId3";
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId, VERSION_ID);
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId2, VERSION_ID);
    dataSetService.addDataSetRevision(providerId, datasetId, revision, representationName, cloudId3, VERSION_ID);

    // when get first page (set page size to 1)
    ResultActions response = mockMvc.perform(
                                        get(dataSetWebTarget, providerId, datasetId, representationName, revisionName, revisionProviderId).
                                            queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp())).
                                            queryParam(F_LIMIT, "1"))
                                    .andExpect(status().isOk());

    // then
    ResultSlice<CloudTagsResponse> slice = responseContentAsCloudTagResultSlice(response);
    List<CloudTagsResponse> cloudIds = slice.getResults();
    assertThat(cloudIds.size(), is(1));
    assertThat(cloudIds.get(0).getCloudId(), is(cloudId));
    assertNotNull(slice.getNextSlice());

    // when get second page
    response = mockMvc.perform(
                          get(dataSetWebTarget, providerId, datasetId, representationName, revisionName, revisionProviderId)
                              .queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp()))
                              .queryParam(F_START_FROM, slice.getNextSlice())
                              .queryParam(F_LIMIT, "1"))
                      .andExpect(status().isOk());

    // then
    slice = responseContentAsCloudTagResultSlice(response);
    cloudIds = slice.getResults();
    assertThat(cloudIds.size(), is(1));
    assertThat(cloudIds.get(0).getCloudId(), is(cloudId2));
    assertNotNull(slice.getNextSlice());

    // when get last page
    response = mockMvc.perform(
                          get(dataSetWebTarget, providerId, datasetId, representationName, revisionName, revisionProviderId)
                              .queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp()))
                              .queryParam(F_START_FROM, slice.getNextSlice())
                              .queryParam(F_LIMIT, "1"))
                      .andExpect(status().isOk());

    // then
    slice = responseContentAsCloudTagResultSlice(response);
    cloudIds = slice.getResults();
    assertThat(cloudIds.size(), is(1));
    assertThat(cloudIds.get(0).getCloudId(), is(cloudId3));
    assertNull(slice.getNextSlice());
  }
}