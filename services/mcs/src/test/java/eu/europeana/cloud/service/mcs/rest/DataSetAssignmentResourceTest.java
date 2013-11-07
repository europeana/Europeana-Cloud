package eu.europeana.cloud.service.mcs.rest;

import java.util.List;
import javax.ws.rs.Path;
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

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;
import static org.junit.Assert.*;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;

/**
 * DataSetAssignmentResourceTest
 */
public class DataSetAssignmentResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private DataSetService dataSetService;

    private RecordService recordService;

    private WebTarget dataSetAssignmentWebTarget;

    private DataProvider dataProvider;

    private DataSet dataSet;

    private Representation rep;


    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedServicesTestContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProviderService = applicationContext.getBean(DataProviderService.class);
        dataSetService = applicationContext.getBean(DataSetService.class);
        recordService = applicationContext.getBean(RecordService.class);
        dataSetAssignmentWebTarget = target(DataSetAssignmentsResource.class.getAnnotation(Path.class).value());
        dataProvider = dataProviderService.createProvider("provident", new DataProviderProperties());
        dataSet = dataSetService.createDataSet(dataProvider.getId(), "dataset", "description");
        rep = recordService.createRepresentation("globalId", dataSet.getId(), dataProvider.getId());
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
    public void shouldReturnErrorWhenRepresentationIsAssignedTwice() {
        // given that representation is already assigned to set
        dataSetService.addAssignment(dataProvider.getId(), dataSet.getId(), rep.getRecordId(), rep.getSchema(), rep.getVersion());

        // when representation (even in different version) is to be assigned to the same data set
        Representation rep2 = recordService.createRepresentation(rep.getRecordId(), rep.getSchema(), rep.getDataProvider());
        dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
                .resolveTemplate(P_PROVIDER, dataProvider.getId())
                .resolveTemplate(P_DATASET, dataSet.getId());
        Entity<Form> assinmentForm = Entity.form(
                new Form(F_GID, rep2.getRecordId()).param(F_REP, rep2.getSchema()).param(F_VER, rep2.getVersion()));
        Response addAssignmentResponse = dataSetAssignmentWebTarget.request().post(assinmentForm);

        // then error should be returned
        assertEquals(Response.Status.CONFLICT.getStatusCode(), addAssignmentResponse.getStatus());
        ErrorInfo errorInfo = addAssignmentResponse.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.REPRESENTATION_ALREADY_IN_SET.toString(), errorInfo.getErrorCode());
    }


    @Test
    public void shouldAddAssignmentForLatestVersion() {
        // given representation and data set in data service

        // when representation is assigned to data set without specifying the version
        dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
                .resolveTemplate(P_PROVIDER, dataProvider.getId())
                .resolveTemplate(P_DATASET, dataSet.getId());
        Entity<Form> assinmentForm = Entity.form(new Form(F_GID, rep.getRecordId()).param(F_REP, rep.getSchema()));
        Response addAssignmentResponse = dataSetAssignmentWebTarget.request().post(assinmentForm);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), addAssignmentResponse.getStatus());

        // then we get representation in latest version
        Representation latestRepresentation = recordService.createRepresentation("globalId", dataSet.getId(), dataProvider.getId());
        List<Representation> representations = dataSetService.listDataSet(dataProvider.getId(), dataSet.getId());
        assertEquals(1, representations.size());
        assertEquals(latestRepresentation, representations.get(0));
    }


    @Test
    public void shouldAddAssignmentForSpecificVersion() {
        // given representation and data set in data service
        Representation latestRepresentation = recordService.createRepresentation("globalId", dataSet.getId(), dataProvider.getId());

        // when representation is assigned to data set in specific version
        dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
                .resolveTemplate(P_PROVIDER, dataProvider.getId())
                .resolveTemplate(P_DATASET, dataSet.getId());
        Entity<Form> assinmentForm = Entity.form(new Form(F_GID, rep.getRecordId()).param(F_REP, rep.getSchema()).param(F_VER, rep.getVersion()));
        Response addAssignmentResponse = dataSetAssignmentWebTarget.request().post(assinmentForm);
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), addAssignmentResponse.getStatus());

        // then we get specified version in dataset
        List<Representation> representations = dataSetService.listDataSet(dataProvider.getId(), dataSet.getId());
        assertEquals(1, representations.size());
        assertEquals(rep, representations.get(0));
    }


    @Test
    public void shouldRemoveAssignment() {
        // given assignment in data set
        dataSetService.addAssignment(dataProvider.getId(), dataSet.getId(), rep.getRecordId(), rep.getSchema(), rep.getVersion());
        assertEquals(1, dataSetService.listDataSet(dataProvider.getId(), dataSet.getId()).size());

        // when assignment is deleted
        dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
                .resolveTemplate(P_PROVIDER, dataProvider.getId())
                .resolveTemplate(P_DATASET, dataSet.getId());
        Response deleteAssignmentResponse = dataSetAssignmentWebTarget
                .queryParam(F_GID, rep.getRecordId())
                .queryParam(F_REP, rep.getSchema()).request().delete();
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), deleteAssignmentResponse.getStatus());

        // then there should be no representation in data set
        assertTrue(dataSetService.listDataSet(dataProvider.getId(), dataSet.getId()).isEmpty());
    }
}
