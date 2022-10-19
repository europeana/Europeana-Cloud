package eu.europeana.aas.acl.repository;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import eu.europeana.aas.acl.RetryableTestContextConfiguration;
import eu.europeana.aas.acl.model.AclEntry;
import eu.europeana.aas.acl.model.AclObjectIdentity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static eu.europeana.aas.acl.repository.Utils.createDefaultTestAOI;
import static eu.europeana.aas.acl.repository.Utils.createTestAclEntry;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {RetryableTestContextConfiguration.class})
public class CassandraAclRepositoryRetryableTest {


    @Spy
    @Autowired
    private Session session;
    private AclObjectIdentity aoi;
    private List<AclEntry> aclEntries;

    private int maxAttemptCount;

    @Autowired
    private AclRepository aclRepository;

    @Before
    public void prepare() {
        maxAttemptCount = CassandraAclRepository.ACL_REPO_DEFAULT_MAX_ATTEMPTS;

        when(session.execute(Mockito.any(Statement.class)))
                .thenThrow(new DriverException("Driver error has occurred!"));

        aoi = createDefaultTestAOI();
        aclEntries = List.of(createTestAclEntry("test", 1),
                createTestAclEntry("test", 2),
                createTestAclEntry("test", 4),
                createTestAclEntry("test", 8));
    }


    @Test
    public void testRetryableAnnotation() {
        Mockito.verify(session, Mockito.times(0))
                .execute(Mockito.any(Statement.class));
        try {
            aclRepository.findAcls(List.of(aoi));
        } catch (Exception ignored) {
        }
        Mockito.verify(session, Mockito.times(maxAttemptCount))
                .execute(Mockito.any(Statement.class));
        try {
            aclRepository.updateAcl(aoi, aclEntries);
        } catch (Exception ignored) {
        }
        Mockito.verify(session, Mockito.times(maxAttemptCount * 2))
                .execute(Mockito.any(Statement.class));
    }
}
