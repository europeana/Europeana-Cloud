package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;

/**
 * Created by pwozniak on 2/20/17.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraRecordDAOTest extends CassandraTestBase {

	@Autowired
	private CassandraRecordDAO recordDAO;

	@Autowired
	private CassandraRecordService cassandraRecordService;

	@Test
	public void shouldReturnOneRepresentationVersionForGivenRevisionNameAndRevisionProvider() throws ProviderNotExistsException, RecordNotExistsException, RevisionIsNotValidException {

		Revision revision = new Revision("revName", "revProvider", new Date(), false, true, false);

		String version = new com.eaio.uuid.UUID().toString();
		recordDAO.addRepresentationRevision("sampleCID", "repName", version, "revProvider", "revName", new Date());


		List<Representation> reps = recordDAO.getAllRepresentationVersionsForRevisionName("sampleCID", "repName", revision, null);
		Assert.assertThat(reps.size(), is(1));
		Assert.assertThat(reps.get(0).getCloudId(), is("sampleCID"));
		Assert.assertThat(reps.get(0).getRepresentationName(), is("repName"));
		Assert.assertThat(reps.get(0).getVersion(), is(version));
	}

	@Test
	public void shouldReturnAllVersionsForGivenRevisionNameAndRevisionProvider() throws ProviderNotExistsException, RecordNotExistsException, RevisionIsNotValidException {
		Revision revision = new Revision("revName", "revProvider", new Date(), false, true, false);

		String version_0 = new com.eaio.uuid.UUID().toString();
		String version_1 = new com.eaio.uuid.UUID().toString();
		String version_2 = new com.eaio.uuid.UUID().toString();
		String version_3 = new com.eaio.uuid.UUID().toString();
		String version_4 = new com.eaio.uuid.UUID().toString();

		recordDAO.addRepresentationRevision("sampleCID", "repName", version_0, "revProvider", "revName", new Date());
		recordDAO.addRepresentationRevision("sampleCID", "repName", version_1, "revProvider", "revName", new Date());
		recordDAO.addRepresentationRevision("sampleCID", "repName", version_2, "revProvider", "revName", new Date());
		recordDAO.addRepresentationRevision("sampleCID", "repName", version_3, "revProvider", "revName", new Date());
		recordDAO.addRepresentationRevision("sampleCID", "repName", version_4, "revProvider", "revName", new Date());


		List<Representation> reps = recordDAO.getAllRepresentationVersionsForRevisionName("sampleCID", "repName", revision, null);

		Assert.assertThat(reps.size(), is(5));
		for (Representation rep : reps) {
			Assert.assertThat(rep.getCloudId(), is("sampleCID"));
			Assert.assertThat(rep.getRepresentationName(), is("repName"));
		}
	}
}
