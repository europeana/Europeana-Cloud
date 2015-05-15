package eu.europeana.cloud.service.mcs.persistent.aspects;

import eu.europeana.cloud.service.mcs.persistent.exception.SwiftConnectionException;
import eu.europeana.cloud.service.mcs.persistent.swift.DBlobStore;
import java.util.ArrayList;
import java.util.List;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.http.HttpResponseException;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/retryBlobStoreExecutorTestContext.xml" })
public class RetryBlobStoreExecutorTest {

    @Autowired
    private DBlobStore dynamicBlobStore;

    @Autowired
    private RetryBlobStoreExecutor retryBlobStoreExecutor;


    public RetryBlobStoreExecutorTest() {
    }


    @Test
    public void shouldFailAfterSingleTry()
            throws Throwable {
        // given
        BlobStore mock1 = mock(BlobStore.class);
        doThrow(mock(HttpResponseException.class)).when(mock1).getBlob("container", "name");
        List<BlobStore> mockblob = new ArrayList<>();
        mockblob.add(mock1);
        dynamicBlobStore.setBlobStores(mockblob);
        // updates number of instances in dynamicBlobStore
        retryBlobStoreExecutor.init();
        try {
            //when
            dynamicBlobStore.getBlob("container", "name");
            //then
            fail("SwiftConnectionException should be thrown.");
        } catch (SwiftConnectionException e) {
            //expected excepton
        }
        verify(mock1, times(1)).getBlob("container", "name");
        verifyNoMoreInteractions(mock1);
    }


    @Test
    public void shouldFailAfterTwoRetries()
            throws Throwable {
        // given
        final List<BlobStore> mockblob = new ArrayList<>();
        final BlobStore mock1 = mock(BlobStore.class);
        doThrow(mock(HttpResponseException.class)).when(mock1).getBlob("container", "name");
        final BlobStore mock2 = mock(BlobStore.class);
        doThrow(mock(HttpResponseException.class)).when(mock2).getBlob("container", "name");
        mockblob.add(mock1);
        mockblob.add(mock2);
        dynamicBlobStore.setBlobStores(mockblob);
        retryBlobStoreExecutor.init();
        try {
            // when
            dynamicBlobStore.getBlob("container", "name");

            // then
            fail("SwiftConnectionException should be thrown.");
        } catch (SwiftConnectionException e) {
            //expected excepton
        }

        verify(mock1, times(1)).getBlob("container", "name");
        verify(mock2, times(1)).getBlob("container", "name");
        verifyNoMoreInteractions(mock1);
        verifyNoMoreInteractions(mock2);
    }


    @Test
    public void shouldThrowExceptionInUnannotatedRetryOnFailureFunction()
            throws Throwable {
        // given
        final List<BlobStore> mockblob = new ArrayList<>();
        final BlobStore mock1 = mock(BlobStore.class);
        final BlobStore mock2 = mock(BlobStore.class);

        doThrow(mock(HttpResponseException.class)).when(mock1).getContext();
        doThrow(mock(HttpResponseException.class)).when(mock2).getContext();
        mockblob.add(mock1);
        mockblob.add(mock2);
        dynamicBlobStore.setBlobStores(mockblob);
        retryBlobStoreExecutor.init();
        for (int i = 0; i < 2; i++) {
            try {
                // when
                dynamicBlobStore.getContext();
                fail();
            } catch (HttpResponseException e) {
            }
        }
        // then
        verify(mock1, atMost(2)).getContext();
        verify(mock2, atMost(2)).getContext();
    }


    @Test
    public void shouldRethrowException()
            throws Throwable {
        // given
        final List<BlobStore> mockblob = new ArrayList<>();
        final BlobStore mock1 = mock(BlobStore.class);
        String exceptionMessage = "test";
        doThrow(new RuntimeException(exceptionMessage)).when(mock1).getBlob("container", "name");
        mockblob.add(mock1);
        dynamicBlobStore.setBlobStores(mockblob);
        retryBlobStoreExecutor.init();

        // when
        try {
            dynamicBlobStore.getBlob("container", "name");
            // then
            fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(exceptionMessage, e.getMessage());
        }
        verify(mock1, times(1)).getBlob("container", "name");
        verifyNoMoreInteractions(mock1);
    }


    @Test
    public void shoudIterateForAllBlobStories()
            throws Throwable {
        // given
        final List<BlobStore> mockblob = new ArrayList<>();
        final BlobStore mock1 = mock(BlobStore.class);
        when(mock1.getBlob("container", "name")).thenReturn(mock(Blob.class));
        final BlobStore mock2 = mock(BlobStore.class);
        when(mock2.getBlob("container", "name")).thenReturn(mock(Blob.class));
        final BlobStore mock3 = mock(BlobStore.class);
        when(mock3.getBlob("container", "name")).thenReturn(mock(Blob.class));
        mockblob.add(mock1);
        mockblob.add(mock2);
        mockblob.add(mock3);
        dynamicBlobStore.setBlobStores(mockblob);
        retryBlobStoreExecutor.init();
        for (int i = 0; i < 50; i++) {
            // when
            dynamicBlobStore.getBlob("container", "name");
        }
        // then
        verify(mock1, atLeast(10)).getBlob("container", "name");
        verify(mock2, atLeast(10)).getBlob("container", "name");
        verify(mock3, atLeast(10)).getBlob("container", "name");
    }


    @Test
    public void shoudSwitchInToSecondBlobStore() {
        // given
        final List<BlobStore> mockblob = new ArrayList<>();
        final BlobStore mock1 = mock(BlobStore.class);
        doThrow(mock(HttpResponseException.class)).when(mock1).getBlob("container", "name");
        final BlobStore mock2 = mock(BlobStore.class);
        final Blob expectedBlob = mock(Blob.class);
        when(mock2.getBlob("container", "name")).thenReturn(expectedBlob);
        final BlobStore mock3 = mock(BlobStore.class);
        doThrow(mock(HttpResponseException.class)).when(mock3).getBlob("container", "name");
        mockblob.add(mock1);
        mockblob.add(mock2);
        mockblob.add(mock3);
        dynamicBlobStore.setBlobStores(mockblob);
        retryBlobStoreExecutor.init();
        Blob resultBlob = null;
        // when
        try {
            resultBlob = dynamicBlobStore.getBlob("container", "name");
        } catch (Throwable e) {

        }
        // then
        verify(mock1, times(1)).getBlob("container", "name");
        verify(mock2, times(1)).getBlob("container", "name");
        verifyNoMoreInteractions(mock1);
        verifyNoMoreInteractions(mock2);
        verifyZeroInteractions(mock3);
        assertEquals(expectedBlob, resultBlob);
    }
}
