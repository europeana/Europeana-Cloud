package eu.europeana.aas.authorization.repository;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import eu.europeana.aas.authorization.RetryableTestContextConfiguration;
import eu.europeana.aas.authorization.model.AclEntry;
import eu.europeana.aas.authorization.model.AclObjectIdentity;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Optional;

import static eu.europeana.aas.authorization.repository.AclUtils.createTestAclEntry;
import static eu.europeana.aas.authorization.repository.AclUtils.createTestAclObjectIdentity;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {RetryableTestContextConfiguration.class})
public class CassandraAclRepositoryRetryableTest {


    @Autowired
    private Session session;
    private AclObjectIdentity aoi;
    private List<AclEntry> aclEntries;

    private int maxAttemptCount;

    @Autowired
    private AclRepository aclRepository;

    @BeforeEach
    void prepare() {
        maxAttemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT)
                .orElse(CassandraAclRepository.ACL_REPO_DEFAULT_MAX_ATTEMPTS);

        when(session.execute(Mockito.any(Statement.class)))
                .thenThrow(new DriverException("Driver error has occurred!"));

        aoi = createTestAclObjectIdentity();
        aclEntries = List.of(createTestAclEntry("test", 1),
                createTestAclEntry("test", 2),
                createTestAclEntry("test", 4),
        createTestAclEntry("test", 8));
  }


    @Test
    void testRetryableAnnotation() {
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
