package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/spiedServicesTestContext.xml" })
public class CassandraSwiftInteractionsTest extends CassandraTestBase {

    @Autowired
    private CassandraRecordService cassandraRecordService;

    @Autowired
    private SwiftContentDAO swiftContentDAO;

    @Autowired
    private UISClientHandler uisHandler;

    private static final String providerId = "provider";

    @After
    public void resetMocks() {
	Mockito.reset(swiftContentDAO);
	Mockito.reset(uisHandler);
    }

    @Test
    public void shouldRemainConsistentWhenSwiftNotWorks() throws Exception {

	Mockito.doReturn(new DataProvider()).when(uisHandler)
		.providerExistsInUIS(providerId);
	Mockito.doReturn(true).when(uisHandler).recordExistInUIS("id");
	// prepare failure
	Mockito.doThrow(new MockException()).when(swiftContentDAO)
		.putContent(anyString(), any(InputStream.class));
	// given representation
	byte[] dummyContent = { 1, 2, 3 };
	File f = new File("content.xml", "application/xml", null, null, 0, null);
	Representation r = cassandraRecordService.createRepresentation("id",
		"dc", providerId);

	// when content is put
	try {
	    cassandraRecordService.putContent(r.getCloudId(),
		    r.getRepresentationName(), r.getVersion(), f,
		    new ByteArrayInputStream(dummyContent));
	} catch (MockException e) {
	    // it's expected
	}

	// then - no file should be present
	Representation fetched = cassandraRecordService.getRepresentation(
		r.getCloudId(), r.getRepresentationName(), r.getVersion());
	Assert.assertTrue(fetched.getFiles().isEmpty());
    }

    static class MockException extends RuntimeException {

    }

}
