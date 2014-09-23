package eu.europeana.cloud.service.mcs.rest;

import java.net.URI;
import java.net.URISyntaxException;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.test.AbstractSecurityTest;

@RunWith(SpringJUnit4ClassRunner.class)
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
	
	private UriInfo uriInfo;

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
		
		uriInfo = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

        Mockito.doReturn(uriBuilder).when(uriInfo).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder).path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder).buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(uriInfo).resolve((URI) Mockito.anyObject());

		ApplicationContext applicationContext = ApplicationContextUtils
				.getApplicationContext();
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

		datasetsResource.createDataSet(uriInfo, PROVIDER_ID, DATASET_ID, DESCRIPTION);
	}

	@Test
	public void shouldBeAbleToCreateDatasetWhenAuthenticated() 
			throws ProviderNotExistsException, DataSetAlreadyExistsException, URISyntaxException {
		
        DataSet dS = new DataSet();
        dS.setId("");
        dS.setProviderId("");
        
        Mockito.when(dataSetService.createDataSet(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(dS);
//        Mockito.when(dataProviderService.updateProvider(Mockito.anyString(), (DataProviderProperties) Mockito.any())).thenReturn(dp);

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		datasetsResource.createDataSet(uriInfo, PROVIDER_ID, DATASET_ID, DESCRIPTION);
	}

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToUpdateDataset()
			throws ProviderNotExistsException, DataSetAlreadyExistsException,
			DataSetNotExistsException, eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException {

		datasetResource.updateDataSet(DATASET_ID, PROVIDER_ID, DESCRIPTION);
	}
	
	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToDeleteDataset()
			throws ProviderNotExistsException, DataSetAlreadyExistsException,
			DataSetNotExistsException {

		datasetResource.deleteDataSet(DATASET_ID, PROVIDER_ID);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToUpdateDataset()
			throws ProviderNotExistsException, DataSetAlreadyExistsException,
			DataSetNotExistsException, eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		datasetResource.updateDataSet(DATASET_ID, PROVIDER_ID, DESCRIPTION);
	}

	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenRandomUserTriesToDeleteDataset()
			throws ProviderNotExistsException, DataSetAlreadyExistsException,
			DataSetNotExistsException {

		login(RANDOM_PERSON, RANDOM_PASSWORD);
		datasetResource.deleteDataSet(DATASET_ID, PROVIDER_ID);
	}

	@Test
	public void shouldBeAbleToDeleteDatasetIfHeIsTheOwner()
			throws ProviderNotExistsException, DataSetAlreadyExistsException,
			DataSetNotExistsException {

		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		datasetsResource.createDataSet(uriInfo, PROVIDER_ID, DATASET_ID, DESCRIPTION);
		datasetResource.deleteDataSet(DATASET_ID, PROVIDER_ID);
	}

	/**
	 * Makes sure Van Persie cannot delete datasets that belong to Cristiano
	 * Ronaldo.
	 */
	@Test(expected = AccessDeniedException.class)
	public void shouldThrowExceptionWhenVanPersieTriesToDeleteRonaldosDatasets()
			throws ProviderNotExistsException, DataSetAlreadyExistsException,
			DataSetNotExistsException {

		login(RONALDO, RONALD_PASSWORD);
		datasetsResource.createDataSet(uriInfo, PROVIDER_ID, DATASET_ID, DESCRIPTION);
		login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
		datasetResource.deleteDataSet(DATASET_ID, PROVIDER_ID);
	}
}
