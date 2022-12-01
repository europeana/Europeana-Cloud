package eu.europeana.aas.authorization.repository;

import static eu.europeana.aas.authorization.repository.AclUtils.createTestAclEntry;
import static eu.europeana.aas.authorization.repository.AclUtils.createTestAclObjectIdentity;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import eu.europeana.aas.authorization.RetryableTestContextConfiguration;
import eu.europeana.aas.authorization.model.AclEntry;
import eu.europeana.aas.authorization.model.AclObjectIdentity;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.test.TestUtils;
import java.util.List;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {RetryableTestContextConfiguration.class})
public class CassandraAclRepositoryRetryableTest {


  @Autowired
  private Session session;
  private AclObjectIdentity aoi;
  private List<AclEntry> aclEntries;

  private int maxAttemptCount;


  @Autowired
  private AclRepository aclRepository;

  @Autowired
  private RetryAspect retryAspect;

  public void changeProxySettings() {

    TestUtils.changeFieldValueForClass(RetryableMethodExecutor.class, "DEFAULT_ACL_ATTEMPTS",
        TestUtils.DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES);
    TestUtils.changeFieldValueForClass(RetryableMethodExecutor.class, "DELAY_BETWEEN_ACL_ATTEMPTS",
        TestUtils.DEFAULT_DELAY_BETWEEN_ATTEMPTS);

    Retryable retryable = mock(Retryable.class);
    when(retryable.delay()).thenReturn(TestUtils.DEFAULT_DELAY_BETWEEN_ATTEMPTS);
    when(retryable.maxAttempts()).thenReturn(TestUtils.DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES);
    when(retryable.errorMessage()).thenReturn("TestErrorMessage");
    try {
      doReturn(retryable)
          .when(retryAspect)
          .getAnnotationForMethodOrClass(any(ProceedingJoinPoint.class));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void prepare() {
    changeProxySettings();
    maxAttemptCount = TestUtils.DEFAULT_MAX_RETRY_COUNT_FOR_TESTS_WITH_RETRIES;

    when(session.execute(Mockito.any(Statement.class)))
        .thenThrow(new DriverException("Driver error has occurred!"));

    aoi = createTestAclObjectIdentity();
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
