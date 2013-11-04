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

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;

@RunWith(JUnitParamsRunner.class)
public class RepresentationResourceTest extends JerseyTest {

	private RecordService recordService;

	@Override
	public Application configure() {
		return new ResourceConfig()
				.registerClasses(RepresentationResource.class)
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
	public void getRepresentation(MediaType mediaType) {
		String globalId = "1";
		String representationName = "DC";
		String version = "1.0";
		String fileName = "1.xml";
		Representation representation = new Representation(globalId,
				representationName, version, null, null, "DLF",
				Arrays.asList(new File(fileName, "text/xml",
						"91162629d258a876ee994e9233b2ad87", "2013-01-01",
						12345, null)), true);
		Representation expected = new Representation(representation);
		expected.setUri(URITools.getVersionUri(getBaseUri(), globalId,
				representationName, version));
		expected.setAllVersionsUri(URITools.getAllVersionsUri(getBaseUri(),
				globalId, representationName));
		expected.setFiles(new ArrayList<File>());
		when(recordService.getRepresentation(globalId, representationName))
				.thenReturn(representation);
		
		Response response = target(
				URITools.getRepresentationPath(globalId, representationName)
						.toString()).request(mediaType).get();

		assertThat(response.getStatus(), is(200));
		assertThat(response.getMediaType(), is(mediaType));
		Representation entity = response.readEntity(Representation.class);
		assertThat(entity, is(expected));
		verify(recordService, times(1)).getRepresentation(globalId,
				representationName);
		verifyNoMoreInteractions(recordService);
	}
}
