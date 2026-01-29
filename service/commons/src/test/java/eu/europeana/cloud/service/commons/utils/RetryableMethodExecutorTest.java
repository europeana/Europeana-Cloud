package eu.europeana.cloud.service.commons.utils;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@ExtendWith(MockitoExtension.class)
class RetryableMethodExecutorTest {

  private static final int RETRY_COUNT = 3;
  private static final int SLEEP_BETWEEN_RETRIES_MS = 200;
  String RESULT = "result";
  String ERROR_MESSAGE = "Test method thrown intentional exception!";

  private final int attemptCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(3);

  @Spy
  public TestDaoWithRetry testDao = new TestDaoWithRetry();

  @Spy
  public TestDaoWithClassLevelRetry testDao2 = new TestDaoWithClassLevelRetry();


  @Mock
  RetryableMethodExecutor.GenericCallable<String, IOException> call;

  @Test
  void shouldReturnValidResultWhenExecuteAndCallNoThrowsExceptions() throws Exception {
    Mockito.when(call.call()).thenReturn(RESULT);

    String result = RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

    assertEquals(RESULT, result);
  }

  @Test
  void shouldCallBeInvokedOnceWhenInvokedExecuteAndCallNoThrowsExceptions() throws Exception {
    Mockito.when(call.call()).thenReturn(RESULT);

    RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

    Mockito.verify(call).call();
  }


  @Test
  void shouldCatchExceptionWhenInvokedExecuteAndCallAlwaysThrowsExceptions() throws Exception {
    Mockito.when(call.call()).thenThrow(IOException.class);

    assertThrows(IOException.class,
            () -> RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call));
  }

  @Test
  void shouldRetryWhenCallAlwaysThrowsException() throws Exception {
    Mockito.when(call.call()).thenThrow(IOException.class);

    try {
      RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);
    } catch (IOException e) {
      e.printStackTrace();
    }
    Mockito.verify(call, Mockito.times(attemptCount)).call();
  }


  @Test
  void shouldRetryOnErrorMethodWithRetryAnnotationWhenExecutedByProxy() {
    TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
    try {
      retryableDao.retryableMethod();
      fail();
    } catch (TestRuntimeExpection ignore) {
    }
    Mockito.verify(testDao, Mockito.times(attemptCount)).retryableMethod();
  }


  @Test
  void shouldNoRetryMethodWithRetryAnnotationWhenExecutedByProxyWhenNoErrors() {
    TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
    retryableDao.noErrorMethod();

    Mockito.verify(testDao, Mockito.times(1)).noErrorMethod();
  }

  @Test
  void shouldNoRetryOnErrorMethodWithoutRetryAnnotationWhenExecutedByProxy() {
    TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
    try {
      retryableDao.noRetryableMethod();
      fail();
    } catch (TestRuntimeExpection ignore) {
    }

    Mockito.verify(testDao, Mockito.times(1)).noRetryableMethod();
  }

  @Test
  void shouldRetryOnErrorClassWithRetryAnnotationWhenExecutedByProxy() {
    TestDaoWithClassLevelRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao2);
    try {
      retryableDao.methodWithoutRetryableAnnotation();
      fail();
    } catch (TestRuntimeExpection ignore) {
    }

    Mockito.verify(testDao2, Mockito.times(attemptCount)).methodWithoutRetryableAnnotation();

  }

  @Test
  void shouldUseOverrideMethodSettingsOnErrorClassWithRetryAnnotationWhenExecutedByProxy() {
    assumeFalse(RetryableMethodExecutor.areRetryParamsOverridden());
    TestDaoWithClassLevelRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao2);
    try {
      retryableDao.methodWithOverridedRetryableAnnotation();
      fail();
    } catch (TestRuntimeExpection ignore) {
    }
    Mockito.verify(testDao2, Mockito.times(1))
            .methodWithOverridedRetryableAnnotation();

  }



}
