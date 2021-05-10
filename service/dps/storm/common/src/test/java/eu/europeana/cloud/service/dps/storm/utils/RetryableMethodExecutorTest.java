package eu.europeana.cloud.service.dps.storm.utils;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        when(call.call()).thenReturn(RESULT);

        String result = RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

        assertEquals(RESULT, result);
    }

    @Test
    public void shouldCallBeInvokedOnceWhenInvokedExecuteAndCallNoThrowsExceptions() throws Exception {
        when(call.call()).thenReturn(RESULT);

        RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

        verify(call).call();
    }


    @Test(expected = IOException.class)
    public void shouldCatchExceptionWhenInvokedExecuteAndCallAlwaysThrowsExceptions() throws Exception {
        when(call.call()).thenThrow(IOException.class);

        RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);
    }

    @Test
    public void shouldCallBeInvoked4TimesWhenInvokedExecuteWith3RetriesAndCallAlwaysThrowsExceptions() throws Exception {
        when(call.call()).thenThrow(IOException.class);


        try {
            RetryableMethodExecutor.execute(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);
        } catch (IOException e) {
            e.printStackTrace();
        }

        verify(call, times(4)).call();
    }

    @Test
    public void shouldRetryOnErrorMethodWithRetryAnnotationWhenExecutedByProxy()  {

        TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
        try {
            retryableDao.retryableMethod();
            fail();
        }catch(TestDaoExpection e){
        }

        verify(testDao,times(3)).retryableMethod();
    }

    @Test
    public void shouldNoRetryMethodWithRetryAnnotationWhenExecutedByProxyWhenNoErrors()  {

        TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
        retryableDao.noErrorMethod();

        verify(testDao,times(1)).noErrorMethod();
    }

    @Test
    public void shouldNoRetryOnErrorMethodWithoutRetryAnnotationWhenExecutedByProxy()  {

        TestDaoWithRetry retryableDao = RetryableMethodExecutor.createRetryProxy(testDao);
        try {
            retryableDao.noRetryableMethod();
            fail();
        }catch(TestDaoExpection e){
        }

        verify(testDao,times(1)).noRetryableMethod();
    }



}
