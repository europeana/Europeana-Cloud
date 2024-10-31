package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestRunner;
import eu.europeana.cloud.test.S3TestHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.F_DESCRIPTION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SET_RESOURCE;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsRepresentationResultSlice;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * DataSetResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetResourceTest extends CassandraBasedAbstractResourceTest {

  // private DataProviderService dataProviderService;
  private DataSetService dataSetService;

  private RecordService recordService;

  private DataProvider dataProvider = new DataProvider();

  private UISClientHandler uisHandler;

  @BeforeClass
  public static void setUp(){
    S3TestHelper.startS3MockServer();
  }

  @Before
  public void mockUp() {
    dataProvider.setId("testprov");
    uisHandler = applicationContext.getBean(UISClientHandler.class);
    Mockito.reset(uisHandler);
    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider(Mockito.anyString());
    Mockito.doReturn(true).when(uisHandler)
           .existsCloudId(Mockito.anyString());
    Mockito.doReturn(true).when(uisHandler)
           .existsProvider(Mockito.anyString());
    dataSetService = applicationContext.getBean(DataSetService.class);
    recordService = applicationContext.getBean(RecordService.class);
    S3TestHelper.cleanUpBetweenTests();
  }
  @AfterClass
  public static void cleanUp() {
    S3TestHelper.stopS3MockServer();
  }


  @Test
  public void shouldUpdateDataset()
      throws Exception {
    // given certain data set in service
    String dataSetId = "dataset";
    String description = "dataset description";
    dataSetService.createDataSet(dataProvider.getId(), dataSetId, "");

    // when you add data set for a provider
    mockMvc.perform(put(DATA_SET_RESOURCE, dataProvider.getId(), dataSetId)
               .contentType(MediaType.APPLICATION_FORM_URLENCODED).param(F_DESCRIPTION, description))
           .andExpect(status().isNoContent());

    // ten this set should be visible in service
    List<DataSet> dataSetsForPrivider = dataSetService.getDataSets(dataProvider.getId(), null, 10000).getResults();
    assertEquals("Expected single dataset in service", 1, dataSetsForPrivider.size());
    DataSet ds = dataSetsForPrivider.get(0);
    assertEquals(dataSetId, ds.getId());
    assertEquals(description, ds.getDescription());
  }

  @Test
  public void shouldDeleteDataset()
      throws Exception {
    // given certain datasets with the same id for different providers
    String dataSetId = "dataset";
    String anotherProvider = "anotherProvider";
    dataSetService.createDataSet(dataProvider.getId(), dataSetId, "");
    DataProvider another = new DataProvider();
    another.setId(anotherProvider);
    //        Mockito.doReturn(another).when(dataProviderDAO).getProvider("anotherProvider");
    dataSetService.createDataSet(anotherProvider, dataSetId, "");

    // when you delete it for one provider
    mockMvc.perform(delete(DATA_SET_RESOURCE, dataProvider.getId(), dataSetId)).andExpect(status().isNoContent());

    // than deleted dataset should not be in service and non-deleted should remain
    assertTrue("Expecting no dataset for provider service",
        dataSetService.getDataSets(dataProvider.getId(), null, 10000).getResults().isEmpty());
    assertEquals("Expecting one dataset", 1, dataSetService.getDataSets(anotherProvider, null, 10000).getResults()
                                                           .size());
  }

  @Test
  public void shouldListRepresentationsFromDataset()
      throws Exception {
    // given data set with assigned record representations
    String dataSetId = "dataset";
    dataSetService.createDataSet(dataProvider.getId(), dataSetId, "");
    Representation r1_1 = insertDummyPersistentRepresentation("1", "dc", dataProvider.getId());
    Representation r1_2 = insertDummyPersistentRepresentation("1", "dc", dataProvider.getId());
    Representation r2_1 = insertDummyPersistentRepresentation("2", "dc", dataProvider.getId());
    Representation r2_2 = insertDummyPersistentRepresentation("2", "dc", dataProvider.getId());
    // when you list dataset contents
    ResultActions response = mockMvc.perform(
                                        get(DATA_SET_RESOURCE, dataProvider.getId(), dataSetId))
                                    .andExpect(status().isOk());
    List<Representation> dataSetContents = responseContentAsRepresentationResultSlice(response).getResults();

    // then you should get assigned records in specified versions or latest (depending on assigmnents)
    assertEquals(4, dataSetContents.size());
  }

  private Representation insertDummyPersistentRepresentation(String cloudId, String schema, String providerId)
      throws Exception {
    Representation r = recordService.createRepresentation(cloudId, schema, providerId, "dataset");
    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null);
    recordService.putContent(cloudId, schema, r.getVersion(), f, new ByteArrayInputStream(dummyContent));

    Representation rep = recordService.persistRepresentation(r.getCloudId(), r.getRepresentationName(), r.getVersion());
    f.setContentUri(new URI("http://localhost/records/" + cloudId + "/representations/" + schema + "/versions/" + r.getVersion()
        + "/files/content.xml"));
    rep.setFiles(Arrays.asList(f));

    return rep;
  }
}
