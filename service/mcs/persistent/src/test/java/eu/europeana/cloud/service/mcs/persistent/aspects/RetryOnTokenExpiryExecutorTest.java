package eu.europeana.cloud.service.mcs.persistent.aspects;

import eu.europeana.cloud.service.mcs.persistent.exception.SwiftConnectionException;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftConnectionProvider;
import java.io.IOException;
import java.io.InputStream;
import org.aspectj.lang.ProceedingJoinPoint;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder.PayloadBlobBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/retryBlobStoreExecutorTestContext.xml" })
public class RetryOnTokenExpiryExecutorTest {

    @Autowired
    private RetryOnTokenExpiryExecutor instance;

    @Autowired
    private BlobStore blobStore;

    @Autowired
    private ContentDAO dao;

    @Autowired
    private SwiftConnectionProvider provider;


    @Before
    public void init() {
        //reset mocks
        reset(provider);
        reset(blobStore);
        //reset spy
        reset(instance);
    }


    @Test
    public void shouldThrowExceptionOnSecondTry_putContent()
            throws IOException, Throwable {
        // given
        final String fileName = "fileName";
        final String result = "result";
        final String conener = "contener";

        final InputStream stream = mock(InputStream.class);
        final Blob blob = mock(Blob.class);
        final PayloadBlobBuilder builder = mock(PayloadBlobBuilder.class);

        when(provider.getBlobStore()).thenReturn(blobStore);
        when(provider.getContainer()).thenReturn(conener);
        when(blobStore.putBlob(any(String.class), any(Blob.class))).thenThrow(new SwiftConnectionException());
        when(blobStore.blobBuilder(any(String.class))).thenReturn(builder);
        when(builder.name(any(String.class))).thenReturn(builder);
        when(builder.payload(any(InputStream.class))).thenReturn(builder);
        when(builder.build()).thenReturn(blob);

        //when
        try {
            dao.putContent(fileName, stream);
            fail("RuntimeException should be thrown.");
        } catch (RuntimeException ex) {
            assertEquals("All instances of Swift are down", ex.getMessage());
        }

        //then
        verify(blobStore, times(2)).putBlob(conener, blob);
        verify(blobStore, times(2)).blobBuilder(fileName);
        verifyNoMoreInteractions(blobStore);
        verify(instance, times(1)).retry(any(ProceedingJoinPoint.class));
        verifyNoMoreInteractions(instance);
    }


    @Test
    public void shouldRetryOnAspect_putContent()
            throws IOException, Throwable {
        // given
        final String fileName = "fileName";
        final String result = "result";
        final String conener = "contener";

        final InputStream stream = mock(InputStream.class);
        final Blob blob = mock(Blob.class);
        final PayloadBlobBuilder builder = mock(PayloadBlobBuilder.class);

        when(provider.getBlobStore()).thenReturn(blobStore);
        when(provider.getContainer()).thenReturn(conener);
        when(blobStore.putBlob(any(String.class), any(Blob.class))).thenThrow(new SwiftConnectionException())
                .thenReturn("result");
        when(blobStore.blobBuilder(any(String.class))).thenReturn(builder);
        when(builder.name(any(String.class))).thenReturn(builder);
        when(builder.payload(any(InputStream.class))).thenReturn(builder);
        when(builder.build()).thenReturn(blob);

        //when
        dao.putContent(fileName, stream);

        //then
        verify(blobStore, times(2)).putBlob(conener, blob);
        verify(blobStore, times(2)).blobBuilder(fileName);
        verifyNoMoreInteractions(blobStore);
        verify(instance, times(1)).retry(any(ProceedingJoinPoint.class));
        verifyNoMoreInteractions(instance);
    }

}
