package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.storm.utils.Retriever;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RetrieverTest {

    String RESULT = "result";
    String ERROR_MESSAGE = "Błąd testowej operacji!";


    @Mock
    Callable<String> call;

    @Test
    public void repeatOnError3Times_callNoThrowsExceptions_validResult() throws Exception {
        when(call.call()).thenReturn(RESULT);

        String result = Retriever.retryOnError3Times(ERROR_MESSAGE, call);

        assertEquals(RESULT, result);
    }

    @Test
    public void repeatOnError3Times_callNoThrowsExceptions_callInvokedOnce() throws Exception {
        when(call.call()).thenReturn(RESULT);

        String result = Retriever.retryOnError3Times(ERROR_MESSAGE, call);

        verify(call).call();
    }



    @Test(expected = IOException.class)
    public void repeatOnError3Times_callAlwaysThrowsExceptions_catchedException() throws Exception {
        when(call.call()).thenThrow(IOException.class);

        Retriever.retryOnError3Times(ERROR_MESSAGE, call);
    }

    @Test
    public void repeatOnError3Timescall_AlwaysThrowsExceptions_callInvoked3Times() throws Exception {
        when(call.call()).thenThrow(IOException.class);


        try {
            Retriever.<String,IOException>retryOnError3Times(ERROR_MESSAGE, call);
        } catch (IOException e) {
            e.printStackTrace();
        }

        verify(call,times(4)).call();
    }


}
