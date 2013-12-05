package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
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

import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;

/**
 * DataSetResourceTest
 */
public class DataSetResourceTest extends JerseyTest {

    private DataProviderService dataProviderService;

    private DataSetService dataSetService;

    private RecordService recordService;

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
        recordService = applicationContext.getBean(RecordService.class);
        dataSetWebTarget = target(DataSetResource.class.getAnnotation(Path.class).value());
        dataProvider = dataProviderService.createProvider("provident", new DataProviderProperties());
    }


    @After
    public void cleanUp() {
        for (DataProvider prov : dataProviderService.getProviders(null, 10000).getResults()) {
            for (DataSet ds : dataSetService.getDataSets(prov.getId(), null, 10000).getResults()) {
                dataSetService.deleteDataSet(prov.getId(), ds.getId());
            }
            dataProviderService.deleteProvider(prov.getId());
        }
    }


    @Test
    public void shouldCreateDataset() {
        String dataSetId = "dataset";
        String description = "dataset description";

        // when you add data set for a provider 
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
            P_DATASET, dataSetId);
        Response createResponse = dataSetWebTarget.request().put(Entity.form(new Form(F_DESCRIPTION, description)));
        assertEquals(Response.Status.CREATED.getStatusCode(), createResponse.getStatus());

        // ten this set should be visible in service
        List<DataSet> dataSetsForPrivider = dataSetService.getDataSets(dataProvider.getId(), null, 10000).getResults();
        assertEquals("Expected single dataset in service", 1, dataSetsForPrivider.size());
        DataSet ds = dataSetsForPrivider.get(0);
        assertEquals(dataSetId, ds.getId());
        assertEquals(description, ds.getDescription());
    }


    @Test
    public void shouldDeleteDataset() {
        // given certain datasets with the same id for different providers
        String dataSetId = "dataset";
        String anotherProvider = "anotherProvider";
        dataSetService.createDataSet(dataProvider.getId(), dataSetId, "");
        dataProviderService.createProvider("anotherProvider", new DataProviderProperties());
        dataSetService.createDataSet(anotherProvider, dataSetId, "");

        // when you delete it for one provider
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
            P_DATASET, dataSetId);
        Response deleteResponse = dataSetWebTarget.request().delete();
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), deleteResponse.getStatus());

        // than deleted dataset should not be in service and non-deleted should remain
        assertTrue("Expecting no dataset for provier service",
            dataSetService.getDataSets(dataProvider.getId(), null, 10000).getResults().isEmpty());
        assertEquals("Expecting one dataset", 1, dataSetService.getDataSets(anotherProvider, null, 10000).getResults()
                .size());
    }


    @Test
    public void shouldListRepresentationsFromDataset() {
        // given data set with assigned record representations
        String dataSetId = "dataset";
        dataSetService.createDataSet(dataProvider.getId(), dataSetId, "");
        Representation r1_1 = insertDummyPersistentRepresentation("1", "dc", dataProvider.getId());
        Representation r1_2 = insertDummyPersistentRepresentation("1", "dc", dataProvider.getId());
        Representation r2_1 = insertDummyPersistentRepresentation("2", "dc", dataProvider.getId());
        Representation r2_2 = insertDummyPersistentRepresentation("2", "dc", dataProvider.getId());
        dataSetService.addAssignment(dataProvider.getId(), dataSetId, r1_1.getRecordId(), r1_1.getSchema(), null);
        dataSetService.addAssignment(dataProvider.getId(), dataSetId, r2_1.getRecordId(), r2_1.getSchema(),
            r2_1.getVersion());

        // when you list dataset contents
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
            P_DATASET, dataSetId);
        Response listDataset = dataSetWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), listDataset.getStatus());
        List<Representation> dataSetContents = listDataset.readEntity(ResultSlice.class).getResults();

        // then you should get assigned records in specified versions or latest (depending on assigmnents)
        assertEquals(2, dataSetContents.size());
        Representation r1FromDataset, r2FromDataset;
        if (dataSetContents.get(0).getRecordId().equals(r1_1.getRecordId())) {
            r1FromDataset = dataSetContents.get(0);
            r2FromDataset = dataSetContents.get(1);
        } else {
            r1FromDataset = dataSetContents.get(1);
            r2FromDataset = dataSetContents.get(0);
        }
        assertEquals(r1_2, r1FromDataset);
        assertEquals(r2_1, r2FromDataset);
    }


    private Representation insertDummyPersistentRepresentation(String cloudId, String schema, String providerId) {
        Representation r = recordService.createRepresentation(cloudId, schema, providerId);
        byte[] dummyContent = { 1, 2, 3 };
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        recordService.putContent(cloudId, schema, r.getVersion(), f, new ByteArrayInputStream(dummyContent));

        return recordService.persistRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
    }
}
