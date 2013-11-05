package eu.europeana.cloud.service.mcs.rest;

import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.glassfish.jersey.client.ClientProperties;
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
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RepresentationNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.VersionNotExistsExceptionMapper;

@RunWith(JUnitParamsRunner.class)
public class RepresentationVersionResourceTest extends JerseyTest {

	private RecordService recordService;

	static final private String globalId = "1";
	static final private String representationName = "DC";
	static final private String version = "1.0";
	static final private String fileName = "1.xml";

	static final private Representation representation = new Representation(
			globalId, representationName, version, null, null, "DLF",
			Arrays.asList(new File(fileName, "text/xml",
					"91162629d258a876ee994e9233b2ad87", "2013-01-01", 12345,
					null)), true);

	@Override
	public Application configure() {
		return new ResourceConfig()
				.registerClasses(RepresentationVersionResource.class)
				.registerClasses(RecordNotExistsExceptionMapper.class)
				.registerClasses(RepresentationNotExistsExceptionMapper.class)
				.registerClasses(VersionNotExistsExceptionMapper.class)
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
	public void testGetRepresentationVersion(MediaType mediaType) {
		Representation expected = new Representation(representation);
		expected.setUri(URITools.getVersionUri(getBaseUri(), globalId,
				representationName, version));
		expected.setAllVersionsUri(URITools.getAllVersionsUri(getBaseUri(),
				globalId, representationName));
		expected.getFiles()
				.get(0)
				.setContentUri(
						URITools.getContetntUri(getBaseUri(), globalId,
								representationName, version, fileName));
		when(
				recordService.getRepresentation(globalId, representationName,
						version))
				.thenReturn(new Representation(representation));

		Response response = target(
				URITools.getVersionPath(globalId, representationName, version)
						.toString()).request(mediaType).get();

		assertThat(response.getStatus(), is(200));
		assertThat(response.getMediaType(), is(mediaType));
		Representation entity = response.readEntity(Representation.class);
		assertThat(entity, is(expected));
		verify(recordService, times(1)).getRepresentation(globalId,
				representationName, version);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	@Parameters(method = "mimeTypes")
	public void testGetLatestRepresentationVersion(MediaType mediaType) {
		when(recordService.getRepresentation(globalId, representationName))
				.thenReturn(new Representation(representation));

		client().property(ClientProperties.FOLLOW_REDIRECTS, false);
		Response response = target(
				URITools.getVersionPath(globalId, representationName,
						ParamConstants.LATEST_VERSION_KEYWORD).toString())
				.request(mediaType).get();

		assertThat(response.getStatus(), is(307));
		assertThat(response.getLocation(), is(URITools.getVersionUri(
				getBaseUri(), globalId, representationName, version)));
		verify(recordService, times(1)).getRepresentation(globalId,
				representationName);
		verifyNoMoreInteractions(recordService);
	}

	@SuppressWarnings("unused")
	private Object[] errors() {
		return $(
				$(new RecordNotExistsException(),
						McsErrorCode.RECORD_NOT_EXISTS.toString()),
				$(new RepresentationNotExistsException(),
						McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()),
				$(new VersionNotExistsException(),
						McsErrorCode.VERSION_NOT_EXISTS.toString()));
	}

	@Test
	@Parameters(method = "errors")
	public void testGetRepresentationVersionReturns404IfRepresentationOrRecordOrVersionDoesNotExists(
			Throwable exception, String errorCode) {
		when(
				recordService.getRepresentation(globalId, representationName,
						version)).thenThrow(exception);

		Response response = target()
				.path(URITools.getVersionPath(globalId, representationName,
						version).toString()).request(MediaType.APPLICATION_XML)
				.get();

		assertThat(response.getStatus(), is(404));
		ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
		assertThat(errorInfo.getErrorCode(), is(errorCode));
		verify(recordService, times(1)).getRepresentation(globalId,
				representationName, version);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void testGetRepresentationVersionReturns406ForUnsupportedFormat() {
		Response response = target()
				.path(URITools.getVersionPath(globalId, representationName,
						version).toString())
				.request(MediaType.APPLICATION_SVG_XML_TYPE).get();

		assertThat(response.getStatus(), is(406));
	}

	@Test
	public void testDeleteRepresentation() {
		Response response = target()
				.path(URITools.getVersionPath(globalId, representationName,
						version).toString()).request().delete();

		assertThat(response.getStatus(), is(204));
		verify(recordService, times(1)).deleteRepresentation(globalId,
				representationName, version);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	@Parameters(method = "errors")
	public void testDeleteRepresentationReturns404IfRecordOrRepresentationDoesNotExists(
			Throwable exception, String errorCode) throws Exception {
		Mockito.doThrow(exception).when(recordService)
				.deleteRepresentation(globalId, representationName, version);

		Response response = target()
				.path(URITools.getVersionPath(globalId, representationName,
						version).toString()).request().delete();

		assertThat(response.getStatus(), is(404));
		ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
		assertThat(errorInfo.getErrorCode(), is(errorCode));
		verify(recordService, times(1)).deleteRepresentation(globalId,
				representationName, version);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void testPersistRepresentation() {
		when(
				recordService.persistRepresentation(globalId,
						representationName, version)).thenReturn(
				new Representation(representation));

		Response response = target(
				URITools.getVersionPath(globalId, representationName, version)
						.toString() + "/persist").request().post(
				Entity.entity(new Form(),
						MediaType.APPLICATION_FORM_URLENCODED_TYPE));

		assertThat(response.getStatus(), is(201));
		assertThat(response.getLocation(), is(URITools.getVersionUri(
				getBaseUri(), globalId, representationName, version)));
		verify(recordService, times(1)).persistRepresentation(globalId,
				representationName, version);
		verifyNoMoreInteractions(recordService);
	}

}
