package eu.europeana.cloud.service.mcs.rest;

import java.net.URI;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;
import static org.junit.Assert.*;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * DataSetResourceTest
 */
public class DataSetResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private DataSetService dataSetService;

    private WebTarget dataSetsWebTarget;

    private WebTarget dataSetWebTarget;

    private WebTarget dataSetAssignmentWebTarget;

    private DataProvider dataProvider;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        dataSetService = applicationContext.getBean(DataSetService.class);
        dataSetsWebTarget = target("/data-providers/{" + P_PROVIDER + "}/data-sets");
        dataSetWebTarget = dataSetsWebTarget.path("{" + P_DATASET + "}");
        dataSetAssignmentWebTarget = dataSetWebTarget.path("assignments");
        dataProvider = dataProviderService.createProvider("provident", new DataProviderProperties());
    }


    @After
    public void cleanUp() {
        for (DataProvider prov : dataProviderService.getProviders()) {
            for (DataSet ds : dataSetService.getDataSets(prov.getId())) {
                dataSetService.deleteDataSet(prov.getId(), ds.getId());
            }
            dataProviderService.deleteProvider(prov.getId());
        }
    }


    @Test
    public void shouldCreateDataset() {
        dataSetsWebTarget = dataSetsWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId());
        Response createResponse = dataSetsWebTarget.request().post(Entity.form(new Form(F_DATASET, "dataset")));
        assertEquals(Response.Status.CREATED.getStatusCode(), createResponse.getStatus());

        URI expectedObjectUri = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(P_DATASET, "dataset").getUri();
        assertEquals(expectedObjectUri, createResponse.getLocation());

        Response getReponse = client().target(expectedObjectUri).request().get();
        assertEquals(Response.Status.OK.getStatusCode(), getReponse.getStatus());
    }


    @Test
    public void shouldNotCreateTwoDatasetsWithSameId() {
        dataSetService.createDataSet(dataProvider.getId(), "dataset");

        dataSetsWebTarget = dataSetsWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId());
        Response createResponse = dataSetsWebTarget.request().post(Entity.form(new Form(F_DATASET, "dataset")));
        assertEquals(Response.Status.CONFLICT.getStatusCode(), createResponse.getStatus());
        ErrorInfo errorInfo = createResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.DATASET_ALREADY_EXISTS.toString(), errorInfo.getErrorCode());
    }


    @Test
    public void shouldNotCreateDatasetForNotexistingProvider() {
        dataSetsWebTarget = dataSetsWebTarget.resolveTemplate(P_PROVIDER, "notexisting");
        Response createResponse = dataSetsWebTarget.request().post(Entity.form(new Form(F_DATASET, "dataset")));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), createResponse.getStatus());
        ErrorInfo errorInfo = createResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.PROVIDER_NOT_EXISTS.toString(), errorInfo.getErrorCode());
    }


//    public void shouldAddAssignment() {
//        dataSetService.createDataSet(dataProvider.getId(), "dataset");
//        
//        dataSetAssignmentWebTarget = dataSetAssignmentWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(P_DATASET, "dataset");
//        
//        dataSetAssignmentWebTarget
//    }
}
