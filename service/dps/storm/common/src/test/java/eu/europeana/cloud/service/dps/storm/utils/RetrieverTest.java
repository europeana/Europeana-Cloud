package eu.europeana.cloud.service.dps.storm.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RetrieverTest {

    private static final int RETRY_COUNT = 3;
    private static final int SLEEP_BETWEEN_RETRIES_MS = 200;
    String RESULT = "result";
    String ERROR_MESSAGE = "Test method thrown intentional exception!";


    @Mock
    Retriever.GenericCallable<String, IOException> call;

    @Test
    public void shouldReturnValidResultWhenRepeatOnErrorAndCallNoThrowsExceptions() throws Exception {
        when(call.call()).thenReturn(RESULT);

        String result = Retriever.retryOnError(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

        assertEquals(RESULT, result);
    }

    @Test
    public void shouldCallBeInvokedOnceWhenInvokedRepeatOnErrorAndCallNoThrowsExceptions() throws Exception {
        when(call.call()).thenReturn(RESULT);

        Retriever.retryOnError(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);

        verify(call).call();
    }


    @Test(expected = IOException.class)
    public void shouldCatchExceptionWhenInvokedRepeatOnErrorAndCallAlwaysThrowsExceptions() throws Exception {
        when(call.call()).thenThrow(IOException.class);

        Retriever.retryOnError(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);
    }

    @Test
    public void shouldCallBeInvoked4TimesWhenInvokedRepeatOnErrorWith3RetriesAndCallAlwaysThrowsExceptions() throws Exception {
        when(call.call()).thenThrow(IOException.class);


        try {
            Retriever.retryOnError(ERROR_MESSAGE, RETRY_COUNT, SLEEP_BETWEEN_RETRIES_MS, call);
        } catch (IOException e) {
            e.printStackTrace();
        }

        verify(call, times(4)).call();
    }

}
