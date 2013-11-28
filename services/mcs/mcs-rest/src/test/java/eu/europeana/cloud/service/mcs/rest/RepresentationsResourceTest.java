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
import java.util.List;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
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

import com.google.common.collect.Lists;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.McsErrorCode;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import java.util.Date;

@RunWith(JUnitParamsRunner.class)
public class RepresentationsResourceTest extends JerseyTest {

	private RecordService recordService;

	static final private String globalId = "1";
	static final private String schema = "DC";
	static final private String version = "1.0";
	static final private Record record = new Record(globalId,
			Lists.newArrayList(new Representation(globalId, schema,
					version, null, null, "DLF", Arrays.asList(new File("1.xml",
							"text/xml", "91162629d258a876ee994e9233b2ad87",
							"2013-01-01", 12345, null)), true, new Date())));

	@Override
	public Application configure() {
		return new ResourceConfig()
				.registerClasses(RepresentationsResource.class)
				.registerClasses(RecordNotExistsExceptionMapper.class)
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
	public void getRepresentations(MediaType mediaType) {
		Record expected = new Record(record);
		Representation expectedRepresentation = expected.getRepresentations()
				.get(0);
		expectedRepresentation.setUri(URITools.getVersionUri(getBaseUri(),
				globalId, schema, version));
		expectedRepresentation.setAllVersionsUri(URITools.getAllVersionsUri(
				getBaseUri(), globalId, schema));
		expectedRepresentation.setFiles(new ArrayList<File>());
		when(recordService.getRecord(globalId)).thenReturn(new Record(record));

		Response response = target(
				URITools.getRepresentationsPath(globalId).toString()).request(
				mediaType).get();

		assertThat(response.getStatus(), is(200));
		assertThat(response.getMediaType(), is(mediaType));
		List<Representation> entity = response
				.readEntity(new GenericType<List<Representation>>() {
				});
		assertThat(entity, is(expected.getRepresentations()));
		verify(recordService, times(1)).getRecord(globalId);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void getRepresentationsReturns404IfRecordDoesNotExists() {
		Throwable exception = new RecordNotExistsException();
		when(recordService.getRecord(globalId)).thenThrow(exception);

		Response response = target()
				.path(URITools.getRepresentationsPath(globalId).toString())
				.request(MediaType.APPLICATION_XML).get();

		assertThat(response.getStatus(), is(404));
		ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
		assertThat(errorInfo.getErrorCode(),
				is(McsErrorCode.RECORD_NOT_EXISTS.toString()));
		verify(recordService, times(1)).getRecord(globalId);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void getRepresentationsReturns406ForUnsupportedFormat() {
		Response response = target()
				.path(URITools.getRepresentationsPath(globalId).toString())
				.request(MediaType.APPLICATION_SVG_XML_TYPE).get();

		assertThat(response.getStatus(), is(406));
	}

}
