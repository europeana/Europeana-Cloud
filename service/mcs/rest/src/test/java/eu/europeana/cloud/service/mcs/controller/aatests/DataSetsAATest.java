package eu.europeana.cloud.service.mcs.controller.aatests;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetDeletionException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.controller.DataSetResource;
import eu.europeana.cloud.service.mcs.controller.DataSetsResource;
import eu.europeana.cloud.test.AbstractSecurityTest;
import javax.validation.constraints.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;

public class DataSetsAATest extends AbstractSecurityTest {

  @Autowired
  private CassandraDataSetService dataSetService;

  @Autowired
  @NotNull
  private DataSetsResource datasetsResource;

  @Autowired
  @NotNull
  private DataSetResource datasetResource;

  @Autowired
  @NotNull
  private UISClientHandler uisHandler;

  private static final String DATASET_ID = "dataset";
  private static final String PROVIDER_ID = "provider";
  private static final String DESCRIPTION = "description";

  /**
   * Pre-defined users
   */
  private final static String RANDOM_PERSON = "Cristiano";
  private final static String RANDOM_PASSWORD = "Ronaldo";

  private final static String VAN_PERSIE = "Robin_Van_Persie";
  private final static String VAN_PERSIE_PASSWORD = "Feyenoord";

  private final static String RONALDO = "Cristiano";
  private final static String RONALD_PASSWORD = "Ronaldo";

  private final static String ADMIN = "admin";
  private final static String ADMIN_PASSWORD = "admin";


  @Before
  public void mockUp() throws Exception {

    DataSet dataset = new DataSet();
    dataset.setId(DATASET_ID);
    dataset.setProviderId(PROVIDER_ID);
    dataset.setDescription(DESCRIPTION);

    // dataProvider.setId("testprov");
    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider(Mockito.anyString());
    Mockito.doReturn(true).when(uisHandler)
           .existsCloudId(Mockito.anyString());
    Mockito.doReturn(true).when(uisHandler)
           .existsProvider(Mockito.anyString());

    Mockito.doReturn(dataset).when(dataSetService).createDataSet(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
  }


  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCreateDataset()
      throws ProviderNotExistsException, DataSetAlreadyExistsException {

    datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION);
  }

  @Test
  public void shouldBeAbleToCreateDatasetWhenAuthenticated()
      throws ProviderNotExistsException, DataSetAlreadyExistsException {

    DataSet dS = new DataSet();
    dS.setId("");
    dS.setProviderId("");

    Mockito.when(dataSetService.createDataSet(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(dS);
    //        Mockito.when(dataProviderService.updateProvider(Mockito.anyString(), (DataProviderProperties) Mockito.any())).thenReturn(dp);

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION);
  }

  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToUpdateDataset()
      throws
      DataSetNotExistsException {

    datasetResource.updateDataSet(PROVIDER_ID, DATASET_ID, DESCRIPTION);
  }

  @Test(expected = AuthenticationCredentialsNotFoundException.class)
  public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteDataset()
      throws DataSetDeletionException, DataSetNotExistsException {

    datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID);
  }

  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenRandomUserTriesToUpdateDataset()
      throws
      DataSetNotExistsException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    datasetResource.updateDataSet(PROVIDER_ID, DATASET_ID, DESCRIPTION);
  }

  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenRandomUserTriesToDeleteDataset()
      throws DataSetDeletionException, DataSetNotExistsException {

    login(RANDOM_PERSON, RANDOM_PASSWORD);
    datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID);
  }

  @Test
  public void shouldBeAbleToDeleteDatasetIfHeIsTheOwner()
      throws ProviderNotExistsException, DataSetAlreadyExistsException, DataSetDeletionException, DataSetNotExistsException {

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION);
    datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID);
  }

  /**
   * Makes sure Van Persie cannot delete datasets that belong to Cristiano Ronaldo.
   */
  @Test(expected = AccessDeniedException.class)
  public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosDatasets()
      throws ProviderNotExistsException, DataSetAlreadyExistsException, DataSetDeletionException, DataSetNotExistsException {

    login(RONALDO, RONALD_PASSWORD);
    datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION);
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID);
  }
}
