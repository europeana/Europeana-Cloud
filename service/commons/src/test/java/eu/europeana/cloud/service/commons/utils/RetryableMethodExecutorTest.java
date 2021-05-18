package eu.europeana.cloud.service.commons.utils;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class RetryableMethodExecutorTest {

    private static final int RETRY_COUNT = 3;
    private static final int SLEEP_BETWEEN_RETRIES_MS = 200;
    String RESULT = "result";
    String ERROR_MESSAGE = "Test method thrown intentional exception!";

    @Spy
    public TestDaoWithRetry testDao=new TestDaoWithRetry();

    @Mock
    RetryableMethodExecutor.GenericCallable<String, IOException> call;

    @Test
    public void shouldReturnValidResultWhenExecuteAndCallNoThrowsExceptions() throws Exception {
        Mockito.when(call.call()).thenReturn(RESULT);

        String result = RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

        assertEquals(RESULT, result);
    }

    @Test
    public void shouldCallBeInvokedOnceWhenInvokedExecuteAndCallNoThrowsExceptions() throws Exception {
        Mockito.when(call.call()).thenReturn(RESULT);

        RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

        Mockito.verify(call).call();
    }


    @Test(expected = IOException.class)
    public void shouldCatchExceptionWhenInvokedExecuteAndCallAlwaysThrowsExceptions() throws Exception {
        Mockito.when(call.call()).thenThrow(IOException.class);

        RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);
    }

    @Test
    public void shouldCallBeInvoked3TimesWhenInvokedExecuteWith3RetriesAndCallAlwaysThrowsExceptions() throws Exception {
        Mockito.when(call.call()).thenThrow(IOException.class);


        try {
            RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Mockito.verify(call, Mockito.times(3)).call();
    }

    @Test
    public void shouldRetryOnErrorMethodWithRetryAnnotationWhenExecutedByProxy()  {

        TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
        try {
            retryableDao.retryableMethod();
            fail();
        }catch(TestRuntimeExpection e){
        }

        Mockito.verify(testDao, Mockito.times(3)).retryableMethod();
    }

    @Test
    public void shouldNoRetryMethodWithRetryAnnotationWhenExecutedByProxyWhenNoErrors()  {

        TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
        retryableDao.noErrorMethod();

        Mockito.verify(testDao, Mockito.times(1)).noErrorMethod();
    }

    @Test
    public void shouldNoRetryOnErrorMethodWithoutRetryAnnotationWhenExecutedByProxy()  {

        TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
        try {
            retryableDao.noRetryableMethod();
            fail();
        }catch(TestRuntimeExpection e){
        }

        Mockito.verify(testDao, Mockito.times(1)).noRetryableMethod();
    }
}
