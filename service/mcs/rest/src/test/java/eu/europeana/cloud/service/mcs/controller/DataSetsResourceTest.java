package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.common.web.ParamConstants.F_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.F_DESCRIPTION;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.responseContentAsErrorInfo;
import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import eu.europeana.cloud.test.CassandraTestRunner;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * DataSetResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetsResourceTest extends CassandraBasedAbstractResourceTest {

  private DataSetService dataSetService;

  private String dataSetsWebTarget;

  private UISClientHandler uisHandler;

  private DataProvider dataProvider = new DataProvider();

  @Before
  public void mockUp() {
    dataProvider.setId("provId");
    dataSetService = applicationContext.getBean(DataSetService.class);
    uisHandler = applicationContext.getBean(UISClientHandler.class);
    dataSetsWebTarget = DataSetsResource.class.getAnnotation(RequestMapping.class).value()[0];
  }

  @After
  public void cleanUp() {
    Mockito.reset(uisHandler);
  }

  @Test
  public void shouldCreateDataset() throws Exception {
    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider("provId");
    // given
    String datasetId = "datasetId";
    String description = "dataset description";

    // when you add data set for a provider
    ResultActions createResponse = mockMvc.perform(post(dataSetsWebTarget,
        "provId").contentType(MediaType.APPLICATION_FORM_URLENCODED)
                 .param(F_DATASET, datasetId).param(F_DESCRIPTION, description)
    ).andExpect(status().isCreated());

    // then location of dataset should be given in response
    String uriFromResponse = createResponse.andReturn().getResponse().getHeader(HttpHeaders.LOCATION);
    assertEquals("http://localhost/data-providers/provId/data-sets/datasetId", uriFromResponse);
    // and then this set should be visible in service
    List<DataSet> dataSetsForPrivider = dataSetService.getDataSets(
        "provId", null, 10000).getResults();
    assertEquals("Expected single dataset in service", 1,
        dataSetsForPrivider.size());
    DataSet ds = dataSetsForPrivider.get(0);
    assertEquals(datasetId, ds.getId());
    assertEquals(description, ds.getDescription());
  }

  @Test
  public void shouldRequireDatasetIdParameterOnCreate() throws Exception {
    // given
    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider("notexisting");
    String description = "dataset description";

    // when you try to add data set without id
    ResultActions createResponse = mockMvc.perform(post(dataSetsWebTarget, "provId")
        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
        .param(F_DESCRIPTION, description)
    ).andExpect(status().isBadRequest());

    // then you should get error
    ErrorInfo errorInfo = responseContentAsErrorInfo(createResponse);
    assertEquals(McsErrorCode.OTHER.toString(), errorInfo.getErrorCode());
  }

  @Test
  public void shouldNotCreateTwoDatasetsWithSameId() throws Exception {
    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider("provId");
    // given that there is a dataset with certain id
    String dataSetId = "dataset";
    dataSetService.createDataSet("provId", dataSetId, "");

    // when you try to add a dataset for the same provider with this id
    ResultActions createResponse = mockMvc.perform(post(dataSetsWebTarget, "provId")
        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
        .param(F_DATASET, dataSetId)).andExpect(status().isConflict());

    // then you should get information about conflict
    ErrorInfo errorInfo = responseContentAsErrorInfo(createResponse);
    assertEquals(McsErrorCode.DATASET_ALREADY_EXISTS.toString(),
        errorInfo.getErrorCode());
  }

  @Test
  public void shouldNotCreateDatasetForNotexistingProvider()
      throws Exception {

    Mockito.doReturn(null).when(uisHandler)
           .getProvider("notexisting");

    // when you try to add dataset to this not existing provider
    ResultActions createResponse = mockMvc.perform(post(dataSetsWebTarget, "notexisting")
        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
        .param(F_DATASET, "dataset")).andExpect(status().isNotFound());

    // then you should get error
    ErrorInfo errorInfo = responseContentAsErrorInfo(createResponse);
    assertEquals(McsErrorCode.PROVIDER_NOT_EXISTS.toString(),
        errorInfo.getErrorCode());
    Mockito.reset(uisHandler);
  }
}
