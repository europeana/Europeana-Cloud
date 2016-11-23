package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.test.CassandraTestRunner;
import java.io.ByteArrayInputStream;
import java.util.List;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.JerseyTest;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

/**
 * DataSetResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetResourceTest extends JerseyTest {

    // private DataProviderService dataProviderService;
    private DataSetService dataSetService;

    private RecordService recordService;

    private WebTarget dataSetWebTarget;

    private WebTarget dataSetAssignmentWebTarget;

    private DataProvider dataProvider = new DataProvider();

    private UISClientHandler uisHandler;

    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }

    @Before
    public void mockUp()
            throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        dataProvider.setId("testprov");
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        Mockito.doReturn(new DataProvider()).when(uisHandler)
                .getProvider(Mockito.anyString());
        Mockito.doReturn(true).when(uisHandler)
                .existsCloudId(Mockito.anyString());
        Mockito.doReturn(true).when(uisHandler)
                .existsProvider(Mockito.anyString());
        dataSetService = applicationContext.getBean(DataSetService.class);
        recordService = applicationContext.getBean(RecordService.class);
        dataSetWebTarget = target(DataSetResource.class.getAnnotation(Path.class).value());
    }

    @Test
    public void shouldUpdateDataset()
            throws Exception {
        // given certain data set in service
        String dataSetId = "dataset";
        String description = "dataset description";
        dataSetService.createDataSet(dataProvider.getId(), dataSetId, "");

        // when you add data set for a provider 
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
                P_DATASET, dataSetId);
        Response updateResponse = dataSetWebTarget.request().put(Entity.form(new Form(F_DESCRIPTION, description)));
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), updateResponse.getStatus());

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
    public void shouldListRepresentationsFromDataset()
            throws Exception {
        // given data set with assigned record representations
        String dataSetId = "dataset";
        dataSetService.createDataSet(dataProvider.getId(), dataSetId, "");
        Representation r1_1 = insertDummyPersistentRepresentation("1", "dc", dataProvider.getId());
        Representation r1_2 = insertDummyPersistentRepresentation("1", "dc", dataProvider.getId());
        Representation r2_1 = insertDummyPersistentRepresentation("2", "dc", dataProvider.getId());
        Representation r2_2 = insertDummyPersistentRepresentation("2", "dc", dataProvider.getId());
        dataSetService.addAssignment(dataProvider.getId(), dataSetId, r1_1.getCloudId(), r1_1.getRepresentationName(),
                null);
        dataSetService.addAssignment(dataProvider.getId(), dataSetId, r2_1.getCloudId(), r2_1.getRepresentationName(),
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
        if (dataSetContents.get(0).getCloudId().equals(r1_1.getCloudId())) {
            r1FromDataset = dataSetContents.get(0);
            r2FromDataset = dataSetContents.get(1);
        } else {
            r1FromDataset = dataSetContents.get(1);
            r2FromDataset = dataSetContents.get(0);
        }
        assertEquals(r1_2, r1FromDataset);
        assertEquals(r2_1, r2FromDataset);
    }

    @Test
    public void shouldReturnErrorForMissingParameters() throws Exception {
        String dataSetId = "dataset";

        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
                P_DATASET, dataSetId).path("/latelyTaggedVersion");
        Response response = dataSetWebTarget.request().get();
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        //
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
                P_DATASET, dataSetId).queryParam(P_CLOUDID,"sampleCloudID");
        response = dataSetWebTarget.request().get();
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        //
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
                P_DATASET, dataSetId).queryParam(P_REPRESENTATIONNAME,"sampleRepName");
        response = dataSetWebTarget.request().get();
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        //
        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
                P_DATASET, dataSetId).queryParam(P_REVISION_NAME,"sampleRevisionName");
        response = dataSetWebTarget.request().get();
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void shouldReturnPropperVersionValue() throws DataSetNotExistsException {
        Mockito.doReturn("sample").when(dataSetService).getLatestVersionForGivenRevision(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
        String dataSetId = "dataset";

        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
                P_DATASET, dataSetId).path("/latelyTaggedVersion").queryParam(P_CLOUDID,"sampleCloudID").queryParam(P_REPRESENTATIONNAME,"sampleRepName").queryParam(P_REVISION_NAME,"sampleRevisionName").queryParam(P_REVISION_PROVIDER_ID,"samplerevProvider");
        Response response = dataSetWebTarget.request().get();
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals("sample",response.readEntity(String.class));
    }

    @Test
    public void shouldReturnNoContent() throws DataSetNotExistsException {
        Mockito.doReturn(null).when(dataSetService)
                .getLatestVersionForGivenRevision(Mockito.anyString(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
        String dataSetId = "dataset";

        dataSetWebTarget = dataSetWebTarget.resolveTemplate(P_PROVIDER, dataProvider.getId()).resolveTemplate(
                P_DATASET, dataSetId).path("/latelyTaggedVersion").queryParam(P_CLOUDID,"sampleCloudID").queryParam(P_REPRESENTATIONNAME,"sampleRepName").queryParam(P_REVISION_NAME,"sampleRevisionName").queryParam(P_REVISION_PROVIDER_ID,"samplerevProvider");
        Response response = dataSetWebTarget.request().get();
        assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    }

    private Representation insertDummyPersistentRepresentation(String cloudId, String schema, String providerId)
            throws Exception {
        Representation r = recordService.createRepresentation(cloudId, schema, providerId);
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        recordService.putContent(cloudId, schema, r.getVersion(), f, new ByteArrayInputStream(dummyContent));

        return recordService.persistRepresentation(r.getCloudId(), r.getRepresentationName(), r.getVersion());
    }
}
