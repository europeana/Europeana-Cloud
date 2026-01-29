package eu.europeana.cloud.service.mcs.controller.aatests;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.controller.DataSetResource;
import eu.europeana.cloud.service.mcs.controller.DataSetsResource;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetDeletionException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.test.AbstractSecurityTest;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class DataSetsAATest extends AbstractSecurityTest {

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


    @BeforeEach
    void mockUp() throws Exception {

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


    @Test
    void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCreateDataset() {
        assertThrows(
                AuthenticationCredentialsNotFoundException.class,
                () -> datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION)
        );
    }

    @Test
    void shouldBeAbleToCreateDatasetWhenAuthenticated()
            throws ProviderNotExistsException, DataSetAlreadyExistsException {

        DataSet dS = new DataSet();
        dS.setId("");
        dS.setProviderId("");

        Mockito.when(dataSetService.createDataSet(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(dS);
        //        Mockito.when(dataProviderService.updateProvider(Mockito.anyString(), (DataProviderProperties) Mockito.any())).thenReturn(dp);

        login(RANDOM_PERSON, RANDOM_PASSWORD);
        datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION);
    }

    @Test
    void shouldThrowExceptionWhenNonAuthenticatedUserTriesToUpdateDataset() {
        assertThrows(
                AuthenticationCredentialsNotFoundException.class,
                () -> datasetResource.updateDataSet(PROVIDER_ID, DATASET_ID, DESCRIPTION)
        );
    }

    @Test
    void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteDataset() {
        assertThrows(
                AuthenticationCredentialsNotFoundException.class,
                () -> datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID)
        );
    }

    @Test
    void shouldThrowExceptionWhenRandomUserTriesToUpdateDataset() {
        login(RANDOM_PERSON, RANDOM_PASSWORD);

        assertThrows(
                AccessDeniedException.class,
                () -> datasetResource.updateDataSet(PROVIDER_ID, DATASET_ID, DESCRIPTION)
        );
    }

    @Test
    void shouldThrowExceptionWhenRandomUserTriesToDeleteDataset() {
        login(RANDOM_PERSON, RANDOM_PASSWORD);

        assertThrows(
                AccessDeniedException.class,
                () -> datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID)
        );
    }

    @Test
    void shouldBeAbleToDeleteDatasetIfHeIsTheOwner()
            throws ProviderNotExistsException, DataSetAlreadyExistsException, DataSetDeletionException, DataSetNotExistsException {

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION);
        datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID);
    }

    /**
     * Makes sure Van Persie cannot delete datasets that belong to Cristiano Ronaldo.
     */
    @Test
    void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosDatasets()
            throws ProviderNotExistsException, DataSetAlreadyExistsException {

        login(RONALDO, RONALD_PASSWORD);
        datasetsResource.createDataSet(URI_INFO, PROVIDER_ID, DATASET_ID, DESCRIPTION);

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);

        assertThrows(
                AccessDeniedException.class,
                () -> datasetResource.deleteDataSet(PROVIDER_ID, DATASET_ID)
        );
    }
}
