package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.hash.Hashing;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 *
 * @author sielski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraRecordServiceTest extends CassandraTestBase {

	@Autowired
	private CassandraRecordService cassandraRecordService;

	@Autowired
	private CassandraDataProviderService cassandraDataProviderService;

	private static final String providerId = "provider";


	@Before
	public void prepareData()
			throws Exception {
		cassandraDataProviderService.createProvider(providerId, new DataProviderProperties());
	}


	@Test
	public void shouldCreateAndGetRepresentation()
			throws Exception {
		Representation r = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

		Representation rFetched = cassandraRecordService.getRepresentation("globalId", "dc", r.getVersion());
		assertThat(rFetched, is(r));
	}


	@Test(expected = RepresentationNotExistsException.class)
	public void shouldNotGetRepresentationIfNoPersistentExists()
			throws Exception {
		Representation r = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation rFetched = cassandraRecordService.getRepresentation("globalId", "dc");
	}


	@Test
	public void shouldGetLatestPersistentRepresentation()
			throws Exception {
		Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation r4 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		Representation r5 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

		Representation rFetched = cassandraRecordService.getRepresentation("globalId", "dc");
		assertThat(rFetched, is(r4));
	}


	@Test(expected = ProviderNotExistsException.class)
	public void shouldNotCreateRepresentationForNotExistingProvider()
			throws Exception {
		cassandraRecordService.createRepresentation("globalId", "dc", "not-existing");
	}


	@Test
	public void shouldListAllRepresentationVersionsInOrder()
			throws Exception {
		Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation r4 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		Representation r5 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation rep_another_schema = cassandraRecordService.createRepresentation("globalId", "jpg", providerId);
		r2.setFiles(Collections.EMPTY_LIST);
		r4.setFiles(Collections.EMPTY_LIST);

		List<Representation> representationVersions = cassandraRecordService.
				listRepresentationVersions("globalId", "dc");
		assertThat(representationVersions, is(Arrays.asList(r5, r4, r3, r2, r1)));
	}


	@Test
	public void shouldReturnWholeRecord()
			throws Exception {
		// only temp representation
		Representation dc = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

		// only persistent representation
		Representation jpg = insertDummyPersistentRepresentation("globalId", "jpg", providerId);

		// mixture of persistent and temp representations
		Representation edm1 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
		Representation edm2 = insertDummyPersistentRepresentation("globalId", "edm", providerId);
		Representation edm3 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
		Representation edm4 = insertDummyPersistentRepresentation("globalId", "edm", providerId);
		Representation edm5 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

		// we will get representations with empty file lists - for comparison, we should remove files from our representations
		jpg.setFiles(Collections.EMPTY_LIST);
		edm4.setFiles(Collections.EMPTY_LIST);

		Record record = cassandraRecordService.getRecord("globalId");
		Set<Representation> expectedRepresentations = new HashSet<>(Arrays.asList(jpg, edm4));
		Set<Representation> fetchedRepresentations = new HashSet<>(record.getRepresentations());
		assertThat(fetchedRepresentations, is(expectedRepresentations));
	}


	@Test
	public void shouldDeleteRepresentationInSpecifiedVersion()
			throws Exception {
		Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

		cassandraRecordService.deleteRepresentation(r1.getRecordId(), r1.getSchema(), r1.getVersion());

		// we will get representations with empty file lists - for comparison, we should remove files from our representations
		r2.setFiles(Collections.EMPTY_LIST);

		List<Representation> representationVersions = cassandraRecordService.
				listRepresentationVersions("globalId", "dc");
		assertThat(representationVersions, is(Arrays.asList(r3, r2)));
	}


	@Test
	public void shouldDeleteAllRepresentationVersions()
			throws Exception {
		Representation r1 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation r2 = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		Representation r3 = cassandraRecordService.createRepresentation("globalId", "dc", providerId);

		cassandraRecordService.deleteRepresentation("globalId", "dc");

		assertTrue(cassandraRecordService.listRepresentationVersions("globalId", "dc").isEmpty());
	}


	@Test
	public void shouldDeleteAllRecord()
			throws Exception {
		// given
		Representation dc = cassandraRecordService.createRepresentation("globalId", "dc", providerId);
		Representation jpg = insertDummyPersistentRepresentation("globalId", "jpg", providerId);
		Representation edm1 = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
		Representation edm2 = insertDummyPersistentRepresentation("globalId", "edm", providerId);

		// when
		cassandraRecordService.deleteRecord("globalId");

		// then
		assertTrue(cassandraRecordService.listRepresentationVersions("globalId", "dc").isEmpty());
	}


	@Test(expected = CannotModifyPersistentRepresentationException.class)
	public void shouldNotDeletePersistentRepresentation()
			throws Exception {
		Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		cassandraRecordService.deleteRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
	}


	@Test(expected = CannotModifyPersistentRepresentationException.class)
	public void shouldNotAddFileToPersistentRepresentation()
			throws Exception {
		Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		byte[] dummyContent = {1, 2, 3};
		File f = new File("content.xml", "application/xml", null, null, 0, null);
		cassandraRecordService.
				putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(dummyContent));
	}


	@Test(expected = CannotModifyPersistentRepresentationException.class)
	public void shouldNotRemoveFileFromPersistentRepresentation()
			throws Exception {
		Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);

		File f = r.getFiles().get(0);
		cassandraRecordService.deleteContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName());
	}


	@Test(expected = CannotPersistEmptyRepresentationException.class)
	public void shouldNotPersistRepresentationWithoutFile()
			throws Exception {
		Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);
		cassandraRecordService.persistRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
	}


	@Test
	public void shouldPutAndGetFile()
			throws Exception {
		Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

		byte[] dummyContent = {1, 2, 3};
		File f = new File("content.xml", "application/xml", null, null, 0, null);
		cassandraRecordService.
				putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(dummyContent));

		r = cassandraRecordService.getRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
		assertThat(r.getFiles().size(), is(1));
		File fetchedFile = r.getFiles().get(0);
		assertThat(fetchedFile.getFileName(), is(f.getFileName()));
		assertThat(fetchedFile.getMimeType(), is(f.getMimeType()));
		assertThat(fetchedFile.getContentLength(), is((long) dummyContent.length));
		String contentMd5 = Hashing.md5().hashBytes(dummyContent).toString();
		assertThat(fetchedFile.getMd5(), is(contentMd5));
	}


	@Test
	public void shouldGetContent()
			throws Exception {
		Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

		byte[] dummyContent = {1, 2, 3};
		File f = new File("content.xml", "application/xml", null, null, 0, null);
		cassandraRecordService.
				putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(dummyContent));
		ByteArrayOutputStream baos = new ByteArrayOutputStream(dummyContent.length);
		cassandraRecordService.getContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName(), baos);

		assertThat(baos.toByteArray(), is(dummyContent));
	}


	@Test
	public void shouldRemoveFile()
			throws Exception {
		//given
		Representation r = cassandraRecordService.createRepresentation("globalId", "edm", providerId);

		byte[] dummyContent = {1, 2, 3};
		File f = new File("content.xml", "application/xml", null, null, 0, null);
		cassandraRecordService.
				putContent(r.getRecordId(), r.getSchema(), r.getVersion(), f, new ByteArrayInputStream(dummyContent));

		// when
		cassandraRecordService.deleteContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName());

		//then
		r = cassandraRecordService.getRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
		assertTrue(r.getFiles().isEmpty());
	}


	public void shouldCopyRepresentation()
			throws Exception {
		Representation r = insertDummyPersistentRepresentation("globalId", "dc", providerId);
		Representation copy = cassandraRecordService.copyRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());

		assertThat(copy, is(r));
		for (File f : r.getFiles()) {
			ByteArrayOutputStream rContent = new ByteArrayOutputStream();
			ByteArrayOutputStream copyContent = new ByteArrayOutputStream();
			cassandraRecordService.getContent(r.getRecordId(), r.getSchema(), r.getVersion(), f.getFileName(), rContent);
			cassandraRecordService.
					getContent(copy.getRecordId(), copy.getSchema(), copy.getVersion(), f.getFileName(), copyContent);
			assertThat(rContent.toByteArray(), is(copyContent.toByteArray()));
		}
	}


	private Representation insertDummyPersistentRepresentation(String cloudId, String schema, String providerId)
			throws Exception {
		Representation r = cassandraRecordService.createRepresentation(cloudId, schema, providerId);
		byte[] dummyContent = {1, 2, 3};
		File f = new File("content.xml", "application/xml", null, null, 0, null);
		cassandraRecordService.
				putContent(cloudId, schema, r.getVersion(), f, new ByteArrayInputStream(dummyContent));

		return cassandraRecordService.persistRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
	}
}
