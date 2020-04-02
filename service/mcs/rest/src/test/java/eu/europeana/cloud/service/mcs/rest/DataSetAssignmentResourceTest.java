package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static org.junit.Assert.*;

/**
 * DataSetAssignmentResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetAssignmentResourceTest extends JerseyTest {

    // private DataProviderService dataProviderService;
    private DataSetService dataSetService;

    private RecordService recordService;

    private WebTarget dataSetAssignmentWebTarget;

    private DataProvider dataProvider = new DataProvider();

    private DataSet dataSet;

    private Representation rep;

    private UISClientHandler uisHandler;

    @Override
    public Application configure() {
	return null; //new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }

    @Before
    public void mockUp() throws Exception {
	ApplicationContext applicationContext = ApplicationContextUtils
		.getApplicationContext();
	dataSetService = applicationContext.getBean(DataSetService.class);
	recordService = applicationContext.getBean(RecordService.class);
	uisHandler = applicationContext.getBean(UISClientHandler.class);
	Mockito.doReturn(new DataProvider()).when(uisHandler)
		.getProvider(Mockito.anyString());
	Mockito.doReturn(true).when(uisHandler)
		.existsCloudId(Mockito.anyString());
	Mockito.doReturn(true).when(uisHandler)
		.existsProvider(Mockito.anyString());
	dataSetAssignmentWebTarget = target(DataSetAssignmentsResource.class
		.getAnnotation(Path.class).value());
	dataProvider.setId("dataProv");
	dataSet = dataSetService.createDataSet(dataProvider.getId(), "dataset",
		"description");
	rep = recordService.createRepresentation("globalId", dataSet.getId(),
		dataProvider.getId());
    }

    @Test
    @Ignore("Now, there second assignment the same representation to data set does not raise error, it just overrides previous version")
    public void shouldReturnErrorWhenRepresentationIsAssignedTwice()
	    throws Exception {
	// given that representation is already assigned to set
	dataSetService
		.addAssignment(dataProvider.getId(), dataSet.getId(),
			rep.getCloudId(), rep.getRepresentationName(),
			rep.getVersion());

	// when representation (even in different version) is to be assigned to
	// the same data set
	Representation rep2 = recordService.createRepresentation(
		rep.getCloudId(), rep.getRepresentationName(),
		rep.getDataProvider());
	dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
		.resolveTemplate(P_PROVIDER, dataProvider.getId())
		.resolveTemplate(P_DATASET, dataSet.getId());
	Entity<Form> assinmentForm = Entity.form(new Form(F_CLOUDID, rep2
		.getCloudId()).param(F_REPRESENTATIONNAME,
		rep2.getRepresentationName()).param(F_VER, rep2.getVersion()));
	Response addAssignmentResponse = dataSetAssignmentWebTarget.request()
		.post(assinmentForm);

	// then error should be returned
	assertEquals(Response.Status.CONFLICT.getStatusCode(),
		addAssignmentResponse.getStatus());
	ErrorInfo errorInfo = addAssignmentResponse.readEntity(ErrorInfo.class);
	assertEquals(McsErrorCode.REPRESENTATION_ALREADY_IN_SET.toString(),
		errorInfo.getErrorCode());
    }

	@Test
	public void shouldAddAssignmentForLatestVersion() throws Exception {
		// given representation and data set in data service
		recordService.putContent(rep.getCloudId(), rep.getRepresentationName(),
				rep.getVersion(), new File("terefere", "xml", null, null, -1,
						null), new ByteArrayInputStream("buf".getBytes()));
		rep = recordService.persistRepresentation(rep.getCloudId(),
				rep.getRepresentationName(), rep.getVersion());

		// when representation is assigned to data set without specifying the
		// version
		dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
				.resolveTemplate(P_PROVIDER, dataProvider.getId())
				.resolveTemplate(P_DATASET, dataSet.getId());
		Entity<Form> assinmentForm = Entity.form(new Form(F_CLOUDID, rep
				.getCloudId()).param(F_REPRESENTATIONNAME,
				rep.getRepresentationName()));
		Response addAssignmentResponse = dataSetAssignmentWebTarget.request()
				.post(assinmentForm);
		assertEquals(Response.Status.NO_CONTENT.getStatusCode(),
				addAssignmentResponse.getStatus());

		// then we get representation in latest version
		Representation latestRepresentation = recordService
				.createRepresentation("globalId", dataSet.getId(),
						dataProvider.getId());
		recordService.putContent(latestRepresentation.getCloudId(),
				latestRepresentation.getRepresentationName(),
				latestRepresentation.getVersion(), new File("terefere", "xml",
						null, null, -1, null),
				new ByteArrayInputStream("buf".getBytes()));
		latestRepresentation = recordService.persistRepresentation(
				latestRepresentation.getCloudId(),
				latestRepresentation.getRepresentationName(),
				latestRepresentation.getVersion());

		List<Representation> representations = dataSetService.listDataSet(
				dataProvider.getId(), dataSet.getId(), null, 10000)
				.getResults();
		assertEquals(1, representations.size());
		assertNotEquals(latestRepresentation, representations.get(0));
		assertEquals(rep, representations.get(0));
	}

    @Test
    public void shouldAddAssignmentForSpecificVersion() throws Exception {
	// given representation and data set in data service
	Representation latestRepresentation = recordService
		.createRepresentation("globalId", dataSet.getId(),
			dataProvider.getId());

	// when representation is assigned to data set in specific version
	dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
		.resolveTemplate(P_PROVIDER, dataProvider.getId())
		.resolveTemplate(P_DATASET, dataSet.getId());
	Entity<Form> assinmentForm = Entity.form(new Form(F_CLOUDID, rep
		.getCloudId()).param(F_REPRESENTATIONNAME,
		rep.getRepresentationName()).param(F_VER, rep.getVersion()));
	Response addAssignmentResponse = dataSetAssignmentWebTarget.request()
		.post(assinmentForm);
	assertEquals(Response.Status.NO_CONTENT.getStatusCode(),
		addAssignmentResponse.getStatus());

	// then we get specified version in dataset
	List<Representation> representations = dataSetService.listDataSet(
		dataProvider.getId(), dataSet.getId(), null, 10000)
		.getResults();
	assertEquals(1, representations.size());
	assertEquals(rep, representations.get(0));
    }

    @Test
    public void shouldRemoveAssignment() throws Exception {
	// given assignment in data set
	dataSetService
		.addAssignment(dataProvider.getId(), dataSet.getId(),
			rep.getCloudId(), rep.getRepresentationName(),
			rep.getVersion());
	assertEquals(
		1,
		dataSetService
			.listDataSet(dataProvider.getId(), dataSet.getId(),
				null, 10000).getResults().size());

	// when assignment is deleted
	dataSetAssignmentWebTarget = dataSetAssignmentWebTarget
		.resolveTemplate(P_PROVIDER, dataProvider.getId())
		.resolveTemplate(P_DATASET, dataSet.getId());
	Response deleteAssignmentResponse = dataSetAssignmentWebTarget
		.queryParam(F_CLOUDID, rep.getCloudId())
		.queryParam(F_REPRESENTATIONNAME, rep.getRepresentationName())
		.queryParam(F_VER, rep.getVersion())
		.request().delete();
	assertEquals(Response.Status.NO_CONTENT.getStatusCode(),
		deleteAssignmentResponse.getStatus());

	// then there should be no representation in data set
	assertTrue(dataSetService
		.listDataSet(dataProvider.getId(), dataSet.getId(), null, 10000)
		.getResults().isEmpty());
    }
}
