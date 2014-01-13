package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ErrorInfo;
import static eu.europeana.cloud.common.web.ParamConstants.F_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.F_DESCRIPTION;
import static eu.europeana.cloud.common.web.ParamConstants.P_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandler;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;
import eu.europeana.cloud.service.uis.dao.InMemoryDataProviderDAO;
import java.net.URI;
import java.util.List;
import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

/**
 * DataSetResourceTest
 */
public class DataSetsResourceTest extends JerseyTest {

    // private DataProviderService dataProviderService;

    private DataSetService dataSetService;

    private WebTarget dataSetsWebTarget;

    private UISClientHandler uisHandler;

    private DataProvider dataProvider = new DataProvider();


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp()
            throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProvider.setId("provId");
        InMemoryDataProviderDAO dataProviderDAO = applicationContext.getBean(InMemoryDataProviderDAO.class);
        Mockito.doReturn(dataProvider).when(dataProviderDAO).getProvider("provId");
        dataSetService = applicationContext.getBean(DataSetService.class);
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        dataSetsWebTarget = target(DataSetsResource.class.getAnnotation(Path.class).value());
    }


    @After
    public void cleanUp()
            throws Exception {

    }


    @Test
    public void shouldCreateDataset()
            throws Exception {
        // given
        String datasetId = "dataset";
        String description = "dataset description";
        // when you add data set for a provider
        dataSetsWebTarget = dataSetsWebTarget.resolveTemplate(P_PROVIDER, "provId");
        Response createResponse = dataSetsWebTarget.request().post(
            Entity.form(new Form(F_DATASET, datasetId).param(F_DESCRIPTION, description)));

        // then location of dataset should be given in response
        assertEquals(Response.Status.CREATED.getStatusCode(), createResponse.getStatus());

        URI expectedObjectUri = dataSetsWebTarget.path("{" + P_DATASET + "}").resolveTemplate(P_PROVIDER, "provId")
                .resolveTemplate(P_DATASET, datasetId).getUri();
        assertEquals(expectedObjectUri, createResponse.getLocation());

        // and then this set should be visible in service
        List<DataSet> dataSetsForPrivider = dataSetService.getDataSets("provId", null, 10000).getResults();
        assertEquals("Expected single dataset in service", 1, dataSetsForPrivider.size());
        DataSet ds = dataSetsForPrivider.get(0);
        assertEquals(datasetId, ds.getId());
        assertEquals(description, ds.getDescription());
    }


    @Test
    public void shouldRequireDatasetIdParameterOnCreate() {
        // given
        String description = "dataset description";

        // when you try to add data set without id
        dataSetsWebTarget = dataSetsWebTarget.resolveTemplate(P_PROVIDER, "provId");
        Response createResponse = dataSetsWebTarget.request().post(Entity.form(new Form(F_DESCRIPTION, description)));

        // then you should get error
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());
        ErrorInfo errorInfo = createResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.OTHER.toString(), errorInfo.getErrorCode());
    }


    @Test
    public void shouldNotCreateTwoDatasetsWithSameId()
            throws Exception {
        // given that there is a dataset with certain id
        String dataSetId = "dataset";
        dataSetService.createDataSet("provId", dataSetId, "");

        // when you try to add a dataset for the same provider with this id
        dataSetsWebTarget = dataSetsWebTarget.resolveTemplate(P_PROVIDER, "provId");
        Response createResponse = dataSetsWebTarget.request().post(Entity.form(new Form(F_DATASET, dataSetId)));

        // then you should get information about conflict
        assertEquals(Response.Status.CONFLICT.getStatusCode(), createResponse.getStatus());
        ErrorInfo errorInfo = createResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.DATASET_ALREADY_EXISTS.toString(), errorInfo.getErrorCode());
    }


    @Test
    public void shouldNotCreateDatasetForNotexistingProvider()
            throws DataSetAlreadyExistsException, ProviderNotExistsException {

        Mockito.doReturn(false).when(uisHandler).providerExistsInUIS("notexisting");

        // when you try to add dataset to this not existing provider
        dataSetsWebTarget = dataSetsWebTarget.resolveTemplate(P_PROVIDER, "notexisting");
        Response createResponse = dataSetsWebTarget.request().post(Entity.form(new Form(F_DATASET, "dataset")));

        // then you should get error
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), createResponse.getStatus());
        ErrorInfo errorInfo = createResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.PROVIDER_NOT_EXISTS.toString(), errorInfo.getErrorCode());
        Mockito.reset(uisHandler);
    }
}
