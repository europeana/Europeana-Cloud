package eu.europeana.cloud.service.mcs.rest;

import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.ProviderNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RepresentationNotExistsExceptionMapper;

@RunWith(JUnitParamsRunner.class)
public class RepresentationResourceTest extends JerseyTest {

	private RecordService recordService;

	static final private String globalId = "1";
	static final private String schema = "DC";
	static final private String version = "1.0";
	static final private String providerID = "DLF";
	static final private Representation representation = new Representation(
			globalId, schema, version, null, null, "DLF",
			Arrays.asList(new File("1.xml", "text/xml",
					"91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
					null)), true);
	static final private Form form = new Form(ParamConstants.F_PROVIDER,
			providerID);

	@Override
	public Application configure() {
		return new ResourceConfig()
				.registerClasses(RepresentationResource.class)
				.registerClasses(RecordNotExistsExceptionMapper.class)
				.registerClasses(RepresentationNotExistsExceptionMapper.class)
				.registerClasses(ProviderNotExistsExceptionMapper.class)
				.property("contextConfigLocation", "classpath:testContext.xml");
	}

	@Before
	public void mockUp() {
		ApplicationContext applicationContext = ApplicationContextUtils
				.getApplicationContext();
		recordService = applicationContext.getBean(RecordService.class);
		Mockito.reset(recordService);
	}

	@SuppressWarnings("unused")
	private Object[] mimeTypes() {
		return $($(MediaType.APPLICATION_XML_TYPE),
				$(MediaType.APPLICATION_JSON_TYPE));
	}

	@Test
	@Parameters(method = "mimeTypes")
	public void getRepresentation(MediaType mediaType) {
		Representation expected = new Representation(representation);
		expected.setUri(URITools.getVersionUri(getBaseUri(), globalId,
				schema, version));
		expected.setAllVersionsUri(URITools.getAllVersionsUri(getBaseUri(),
				globalId, schema));
		expected.setFiles(new ArrayList<File>());
		when(recordService.getRepresentation(globalId, schema))
				.thenReturn(new Representation(representation));

		Response response = target(
				URITools.getRepresentationPath(globalId, schema)
						.toString()).request(mediaType).get();

		assertThat(response.getStatus(), is(200));
		assertThat(response.getMediaType(), is(mediaType));
		Representation entity = response.readEntity(Representation.class);
		assertThat(entity, is(expected));
		verify(recordService, times(1)).getRepresentation(globalId,
				schema);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void getRepresentationReturns406ForUnsupportedFormat() {
		Response response = target()
				.path(URITools.getRepresentationPath(globalId,
						schema).toString())
				.request(MediaType.APPLICATION_SVG_XML_TYPE).get();

		assertThat(response.getStatus(), is(406));
	}

	@SuppressWarnings("unused")
	private Object[] errors() {
		return $(
				$(new RepresentationNotExistsException(),
						McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()),
				$(new RecordNotExistsException(),
						McsErrorCode.RECORD_NOT_EXISTS.toString()));
	}

	@Test
	@Parameters(method = "errors")
	public void getRepresentationReturns404IfRepresentationOrRecordDoesNotExists(
			Throwable exception, String errorCode) {
		when(recordService.getRepresentation(globalId, schema))
				.thenThrow(exception);

		Response response = target()
				.path(URITools.getRepresentationPath(globalId,
						schema).toString())
				.request(MediaType.APPLICATION_XML).get();

		assertThat(response.getStatus(), is(404));
		ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
		assertThat(errorInfo.getErrorCode(), is(errorCode));
		verify(recordService, times(1)).getRepresentation(globalId,
				schema);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void deleteRecord() {
		Response response = target()
				.path(URITools.getRepresentationPath(globalId,
						schema).toString()).request().delete();

		assertThat(response.getStatus(), is(204));
		verify(recordService, times(1)).deleteRepresentation(globalId,
				schema);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	@Parameters(method = "errors")
	public void deleteRepresentationReturns404IfRecordOrRepresentationDoesNotExists(
			Throwable exception, String errorCode) throws Exception {
		Mockito.doThrow(exception).when(recordService)
				.deleteRepresentation(globalId, schema);

		Response response = target()
				.path(URITools.getRepresentationPath(globalId,
						schema).toString()).request().delete();

		assertThat(response.getStatus(), is(404));
		ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
		assertThat(errorInfo.getErrorCode(), is(errorCode));
		verify(recordService, times(1)).deleteRepresentation(globalId,
				schema);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void createRepresentation() {
		when(
				recordService.createRepresentation(globalId,
						schema, providerID)).thenReturn(
				new Representation(representation));

		Response response = target(
				URITools.getRepresentationPath(globalId, schema)
						.toString()).request()
				.post(Entity.entity(form,
						MediaType.APPLICATION_FORM_URLENCODED_TYPE));

		assertThat(response.getStatus(), is(201));
		assertThat(response.getLocation(), is(URITools.getVersionUri(
				getBaseUri(), globalId, schema, version)));
		verify(recordService, times(1)).createRepresentation(globalId,
				schema, providerID);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	@Parameters(method = "errors")
	public void createRepresentationReturns404IfRecordOrRepresentationDoesNotExists(
			Throwable exception, String errorCode) throws Exception {
		Mockito.doThrow(exception).when(recordService)
				.createRepresentation(globalId, schema, providerID);

		Response response = target()
				.path(URITools.getRepresentationPath(globalId,
						schema).toString())
				.request()
				.post(Entity.entity(form,
						MediaType.APPLICATION_FORM_URLENCODED_TYPE));

		assertThat(response.getStatus(), is(404));
		ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
		assertThat(errorInfo.getErrorCode(), is(errorCode));
		verify(recordService, times(1)).createRepresentation(globalId,
				schema, providerID);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void createRepresentationReturns404IfProviderIdIsNotGiven()
			throws Exception {
		Response response = target()
				.path(URITools.getRepresentationPath(globalId,
						schema).toString())
				.request()
				.post(Entity.entity(new Form(),
						MediaType.APPLICATION_FORM_URLENCODED_TYPE));

		assertThat(response.getStatus(), is(400));
		ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
		assertThat(errorInfo.getErrorCode(), is(McsErrorCode.OTHER.toString()));
	}
}
