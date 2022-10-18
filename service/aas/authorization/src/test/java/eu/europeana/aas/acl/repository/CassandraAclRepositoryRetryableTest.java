package eu.europeana.aas.acl.repository;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import eu.europeana.aas.acl.RetryableTestContextConfiguration;
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
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {RetryableTestContextConfiguration.class})
public class CassandraAclRepositoryRetryableTest {


    @Spy
    @Autowired
    private Session session;
    private AclObjectIdentity aoi;

    @Autowired
    private AclRepository aclRepository;

    @Before
    public void prepare() {
        when(session.execute(Mockito.any(Statement.class)))
                .thenThrow(new DriverException("Driver error has occurred!"));

        aoi = createDefaultTestAOI();

    }


    @Test
    public void testRetryableAnnotation() {
        int maxAttemptCount = CassandraAclRepository.ACL_REPO_DEFAULT_MAX_ATTEMPTS;

        Mockito.verify(session, Mockito.times(0))
                .execute(Mockito.any(Statement.class));
        try {
            aclRepository.findAcls(List.of(aoi));
        } catch (Exception ignored) {
        }
        Mockito.verify(session, Mockito.times(maxAttemptCount))
                .execute(Mockito.any(Statement.class));
        try {
            aclRepository.saveAcl(aoi);
        } catch (Exception ignored) {
        }
        Mockito.verify(session, Mockito.times(maxAttemptCount * 2))
                .execute(Mockito.any(Statement.class));
    }
}
