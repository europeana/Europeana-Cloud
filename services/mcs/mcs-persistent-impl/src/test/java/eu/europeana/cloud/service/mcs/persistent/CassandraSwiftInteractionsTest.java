package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Matchers.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author sielski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraSwiftInteractionsTest extends CassandraTestBase {

	@Autowired
	private CassandraRecordService cassandraRecordService;

	@Autowired
	private CassandraDataProviderService cassandraDataProviderService;

	@Autowired
	private SwiftContentDAO swiftContentDAO;

	private static final String providerId = "provider";


	@Before
	public void prepareData() {
		cassandraDataProviderService.createProvider(providerId, new DataProviderProperties());
	}


	@Test
	public void shouldRemainConsistentWhenSwiftNotWorks()
			throws IOException {
		// prepare failure
		Mockito.doThrow(new MockException()).when(swiftContentDAO).
				putContent(anyString(), any(InputStream.class));

		// given representation
		byte[] dummyContent = {1, 2, 3};
		File f = new File("content.xml", "application/xml", null, null, 0, null);
		Representation r = cassandraRecordService.createRepresentation("id", "dc", providerId);

		// when content is put 
		try {
			cassandraRecordService.
					putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(dummyContent));
		} catch (MockException e) {
		// it's expected
		}
		
		// then - no file should be present
		Representation fetched = cassandraRecordService.getRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
		Assert.assertTrue(fetched.getFiles().isEmpty());
	}

	static class MockException extends RuntimeException {

	}

}
