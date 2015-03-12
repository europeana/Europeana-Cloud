package eu.europeana.cloud.service.dps.rest;

import java.net.URI;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.service.dps.DpsService;
import eu.europeana.cloud.service.dps.DpsTask;

@RunWith(SpringJUnit4ClassRunner.class)
public class DpsResourceAATest extends AbstractSecurityTest {

	@Autowired
	@NotNull
	private DpsResource dpsResource;

	@Autowired
	@NotNull
	private DpsService dpsService;

	private static final String GLOBAL_ID = "GLOBAL_ID";

	private UriInfo URI_INFO;

	/**
	 * Pre-defined users
	 */
	private final static String RANDOM_PERSON = "Cristiano";
	private final static String RANDOM_PASSWORD = "Ronaldo";

	private final static String VAN_PERSIE = "Robin_Van_Persie";
	private final static String VAN_PERSIE_PASSWORD = "Feyenoord";

	private final static String RONALDO = "Cristiano";
	private final static String RONALD_PASSWORD = "Ronaldo";

	private final static String ADMIN = "admin";
	private final static String ADMIN_PASSWORD = "admin";

	@Before
	public void mockUp() throws Exception {
		//
		// record = new Record();
		// record.setCloudId(GLOBAL_ID);
		//
		URI_INFO = Mockito.mock(UriInfo.class);
		UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

		Mockito.doReturn(uriBuilder).when(URI_INFO).getBaseUriBuilder();
		Mockito.doReturn(uriBuilder).when(uriBuilder)
				.path((Class) Mockito.anyObject());
		Mockito.doReturn(new URI("")).when(uriBuilder)
				.buildFromMap(Mockito.anyMap());
		Mockito.doReturn(new URI("")).when(uriBuilder)
				.buildFromMap(Mockito.anyMap());
		Mockito.doReturn(new URI("")).when(URI_INFO)
				.resolve((URI) Mockito.anyObject());

		// Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
	}

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToSubmitTask() {

		DpsTask t = new DpsTask("xsltTask");
		String topology = "xsltTopology";

		dpsResource.submitTask(t, topology);
	}

}
