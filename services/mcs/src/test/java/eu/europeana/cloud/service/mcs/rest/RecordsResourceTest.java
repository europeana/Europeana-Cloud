package eu.europeana.cloud.service.mcs.rest;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static junitparams.JUnitParamsRunner.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Application;
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
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.service.RecordService;

@RunWith(JUnitParamsRunner.class)
public class RecordsResourceTest extends JerseyTest {

	private RecordService recordService;

	@Override
	public Application configure() {
		return new ResourceConfig().registerClasses(RecordsResource.class)
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

	@Test
	@Parameters(method = "mimeTypes")
	public void getRecord(MediaType mediaType) {
		String globalId = "global1";
		Record record = newRecord(
				globalId,
				Lists.newArrayList(newRepresentation(
						globalId,
						"DC",
						"1",
						null,
						null,
						"FBC",
						Lists.newArrayList(newFile(
								"dc.xml",
								"text/xml",
								"91162629d258a876ee994e9233b2ad87",
								"2013-10-21",
								12345L,
								URI.create("http://examplecloud.eu/records/"
										+ globalId
										+ "/representations/DC/versions/1/dc.xml"))),
						false)));
		Record expected = newRecord(record.getId(), record.getRepresentations());
		Representation representation = expected.getRepresentations().get(0);
		representation.setRecordId(null);
		representation.setFiles(null);
		representation.setAllVersionsUri(URI.create(getBaseUri() + "records/"
				+ globalId + "/representations/DC/versions"));
		representation.setSelfUri(URI.create(getBaseUri() + "records/"
				+ globalId + "/representations/DC/versions/1"));
		when(recordService.getRecord(globalId)).thenReturn(record);

		Response response = target().path("/records/" + globalId)
				.request(mediaType).get();

		assertThat(response.getStatus(), is(200));
		assertThat(response.getMediaType(), is(mediaType));
		Object entity = response.readEntity(Record.class);
		assertThat((Record) entity, is(expected));
		verify(recordService, times(1)).getRecord(globalId);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void getRecordReturns406ForUnsupportedFormat() {
		Response response = target().path("/records/global1")
				.request(MediaType.APPLICATION_SVG_XML_TYPE).get();

		assertThat(response.getStatus(), is(406));
	}

	@SuppressWarnings("unused")
	private Object[] mimeTypes() {
		return $($(MediaType.APPLICATION_XML_TYPE),
				$(MediaType.APPLICATION_JSON_TYPE));
	}

	@Test
	public void getRecordReturns404IfRecordDoesNotExists() {
		String globalId = "global1";
		Throwable exception = new RecordNotExistsException();
		when(recordService.getRecord(globalId)).thenThrow(exception);

		Response response = target().path("/records/" + globalId)
				.request(MediaType.APPLICATION_XML).get();

		assertThat(response.getStatus(), is(404));
		verify(recordService, times(1)).getRecord(globalId);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void deleteRecord() {
		String globalId = "global1";

		Response response = target().path("/records/" + globalId).request()
				.delete();

		assertThat(response.getStatus(), is(204));
		verify(recordService, times(1)).deleteRecord(globalId);
		verifyNoMoreInteractions(recordService);
	}

	@Test
	public void deleteRecordReturns404IfRecordDoesNotExists() throws Exception {
		String globalId = "global1";
		Throwable exception = new RecordNotExistsException();
		Mockito.doThrow(exception).when(recordService).deleteRecord(globalId);

		Response response = target().path("/records/" + globalId)
				.request(MediaType.APPLICATION_XML).delete();

		assertThat(response.getStatus(), is(404));
		verify(recordService, times(1)).deleteRecord(globalId);
		verifyNoMoreInteractions(recordService);
	}

	private static Record newRecord(final String globalId,
			final List<Representation> representations) {
		Record record = new Record();
		record.setId(globalId);
		List<Representation> newRepresentations = null;
		if (representations != null) {
			newRepresentations = new ArrayList<>(representations.size());
			for (Representation representation : representations) {
				newRepresentations.add(newRepresentation(
						representation.getRecordId(),
						representation.getSchema(),
						representation.getVersion(),
						representation.getAllVersionsUri(),
						representation.getSelfUri(),
						representation.getDataProvider(),
						representation.getFiles(),
						representation.isPersistent()));
			}
		}
		record.setRepresentations(newRepresentations);
		return record;
	}

	private static Representation newRepresentation(final String recordId,
			final String schema, final String version,
			final URI allVersionsUri, final URI selfUri,
			final String dataProvider, final List<File> files,
			final boolean persistent) {
		Representation representation = new Representation();
		representation.setRecordId(recordId);
		representation.setSchema(schema);
		representation.setVersion(version);
		representation.setAllVersionsUri(allVersionsUri);
		representation.setSelfUri(selfUri);
		representation.setDataProvider(dataProvider);
		List<File> newFiles = null;
		if (files != null) {
			newFiles = new ArrayList<>(files.size());
			for (File file : files) {
				newFiles.add(newFile(file.getFileName(), file.getMimeType(),
						file.getMd5(), file.getDate(), file.getContentLength(),
						file.getContentUri()));
			}
		}
		representation.setFiles(newFiles);
		representation.setPersistent(persistent);
		return representation;
	}

	private static File newFile(final String fileName, final String mimeType,
			final String md5, final String date, final long contentLength,
			final URI contentUri) {
		File file = new File();
		file.setFileName(fileName);
		file.setMimeType(mimeType);
		file.setMd5(md5);
		file.setDate(date);
		file.setContentLength(contentLength);
		file.setContentUri(contentUri);
		return file;
	}
}
