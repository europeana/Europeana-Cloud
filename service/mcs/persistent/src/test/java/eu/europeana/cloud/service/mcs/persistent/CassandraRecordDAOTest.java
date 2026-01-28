package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.context.SpiedServicesTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Date;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Created by pwozniak on 2/20/17.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpiedServicesTestContext.class})
public class CassandraRecordDAOTest extends CassandraTestBase {

  @Autowired
  private CassandraRecordDAO recordDAO;

  @Autowired
  private CassandraRecordService cassandraRecordService;

  @Test
  void shouldReturnOneRepresentationVersionForGivenRevisionNameAndRevisionProvider() {

    Revision revision = new Revision("revName", "revProvider", new Date(), false);

    String version = new com.eaio.uuid.UUID().toString();
    recordDAO.addRepresentationRevision("sampleCID", "repName", version, "revProvider", "revName", new Date());

    List<Representation> reps = recordDAO.getAllRepresentationVersionsForRevisionName("sampleCID", "repName", revision, null);
    assertThat(reps.size(), is(1));
    assertThat(reps.get(0).getCloudId(), is("sampleCID"));
    assertThat(reps.get(0).getRepresentationName(), is("repName"));
    assertThat(reps.get(0).getVersion(), is(version));
  }

  @Test
  public void shouldReturnAllVersionsForGivenRevisionNameAndRevisionProvider() {
    Revision revision = new Revision("revName", "revProvider", new Date(), false);

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

    assertThat(reps.size(), is(5));
    for (Representation rep : reps) {
      assertThat(rep.getCloudId(), is("sampleCID"));
      assertThat(rep.getRepresentationName(), is("repName"));
    }
  }

  @Test
  public void shouldRemoveAllRelevantEntriesFromRepresentationRevisionsTable() {
    String version_0 = new com.eaio.uuid.UUID().toString();
    //
    Representation rep = new Representation();
    rep.setCloudId("sampleCloudId");
    rep.setRepresentationName("sampleRepName");
    rep.setVersion(version_0);
    //
    Date d = new Date();
    Revision revision_1 = new Revision("revName", "revProvider", d, false);
    rep.getRevisions().add(revision_1);
    Revision revision_2 = new Revision("revName_1", "revProvider_1", d, false);
    rep.getRevisions().add(revision_2);
    //
    File file = new File("sampleFileName", "application/xml", "md5", "date", 12, null, Storage.DATA_BASE);
    rep.getFiles().add(file);
    //

    recordDAO.addRepresentationRevision(rep.getCloudId(), rep.getRepresentationName(), version_0,
        rep.getRevisions().get(0).getRevisionProviderId(), rep.getRevisions().get(0).getRevisionName(),
        rep.getRevisions().get(0).getCreationTimeStamp());
    recordDAO.addRepresentationRevision(rep.getCloudId(), rep.getRepresentationName(), version_0,
        rep.getRevisions().get(1).getRevisionProviderId(), rep.getRevisions().get(1).getRevisionName(),
        rep.getRevisions().get(1).getCreationTimeStamp());

    recordDAO.addOrReplaceFileInRepresentationRevision(rep.getCloudId(), rep.getRepresentationName(), version_0,
        rep.getRevisions().get(0).getRevisionProviderId(), rep.getRevisions().get(0).getRevisionName(),
        rep.getRevisions().get(0).getCreationTimeStamp(), file);
    recordDAO.addOrReplaceFileInRepresentationRevision(rep.getCloudId(), rep.getRepresentationName(), version_0,
        rep.getRevisions().get(1).getRevisionProviderId(), rep.getRevisions().get(1).getRevisionName(),
            rep.getRevisions().get(1).getCreationTimeStamp(), file);

    //first check
    List<RepresentationRevisionResponse> response1 = recordDAO.getRepresentationRevisions(rep.getCloudId(),
            rep.getRepresentationName(), rep.getRevisions().get(0).getRevisionProviderId(),
            rep.getRevisions().get(0).getRevisionName(), rep.getRevisions().get(0).getCreationTimeStamp());
    List<RepresentationRevisionResponse> response2 = recordDAO.getRepresentationRevisions(rep.getCloudId(),
            rep.getRepresentationName(), rep.getRevisions().get(1).getRevisionProviderId(),
            rep.getRevisions().get(1).getRevisionName(), rep.getRevisions().get(1).getCreationTimeStamp());

    assertThat(response1.get(0).getFiles().size(), is(1));
    assertThat(response2.get(0).getFiles().size(), is(1));

    recordDAO.removeFileFromRepresentationRevisionsTable(rep, file.getFileName());

    //second check
    response1 = recordDAO.getRepresentationRevisions(rep.getCloudId(), rep.getRepresentationName(),
            rep.getRevisions().get(0).getRevisionProviderId(), rep.getRevisions().get(0).getRevisionName(),
            rep.getRevisions().get(0).getCreationTimeStamp());
    response2 = recordDAO.getRepresentationRevisions(rep.getCloudId(), rep.getRepresentationName(),
            rep.getRevisions().get(1).getRevisionProviderId(), rep.getRevisions().get(1).getRevisionName(),
            rep.getRevisions().get(1).getCreationTimeStamp());

    assertThat(response1.get(0).getFiles().size(), is(0));
    assertThat(response2.get(0).getFiles().size(), is(0));
  }
}