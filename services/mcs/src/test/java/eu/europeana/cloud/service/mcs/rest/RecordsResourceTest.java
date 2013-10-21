package eu.europeana.cloud.service.mcs.rest;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.service.RecordService;

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
	public void getRecord() {
		String globalId = "global1";
		Record record = newRecord(globalId);
		when(recordService.getRecord(globalId)).thenReturn(record);

		Response response = target().path("/records/" + globalId)
				.request(MediaType.APPLICATION_XML).get();

		assertThat(response.getStatus(), is(200));
		assertThat(response.getMediaType(), is(MediaType.APPLICATION_XML_TYPE));
		Object entity = response.readEntity(Record.class);
		assertThat((Record) entity, is(record));
		verify(recordService, times(1)).getRecord(globalId);
		verifyNoMoreInteractions(recordService);
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

	private static Record newRecord(String globalId) {
		Record record = new Record();
		record.setId(globalId);
		record.setRepresentations(new ArrayList<Representation>());
		return record;
	}

}
