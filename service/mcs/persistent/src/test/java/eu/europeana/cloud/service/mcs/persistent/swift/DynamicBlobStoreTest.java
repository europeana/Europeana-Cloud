package eu.europeana.cloud.service.mcs.persistent.swift;

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.junit.Test;
import org.junit.Before;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamicBlobStoreTest {

    private DynamicBlobStore instance;
    private List<BlobStore> blobStores;
    private Iterator<BlobStore> blobIterator;


    @Before
    public void init() {
        blobStores = new ArrayList<>();
        blobStores.add(mock(BlobStore.class));
        blobStores.add(mock(BlobStore.class));
        blobStores.add(mock(BlobStore.class));
        blobStores.add(mock(BlobStore.class));
        blobIterator = Iterators.cycle(blobStores);
        instance = new DynamicBlobStore(blobStores, blobIterator);
    }


    @Test
    public void shouldDistibuteLoadUniform() {
        // given
        final int iterations = 20;
        mockGetContextOperation();
        // when
        for (int i = 0; i < iterations; i++) {
            instance.getContext();
        }
        // then
        verifyEqualLoadBalance(iterations);
    }


    @Test
    public void shouldDistibuteLoadUniformWithSingleRetry() {
        // given
        final int iterations = 20;
        mockGetContextOperation();
        // when
        for (int i = 0; i < iterations; i++) {
            if (i == 2) {
                instance.getDynamicBlobStoreWithoutActiveInstance().getContext();
            } else {
                instance.getContext();
            }
        }
        // then
        verifyFairLoadBalance(iterations, 1);

    }


    @Test
    public void shouldDistibuteLoadUniformWithMultiRetries() {
        // given
        final int iterations = 40;
        mockGetContextOperation();
        // when
        for (int i = 0; i < iterations; i++) {
            if (i == 2 || i == 6) {
                instance.getDynamicBlobStoreWithoutActiveInstance().getContext();
            } else {
                instance.getContext();
            }

        }
        // then
        verifyFairLoadBalance(iterations, 2);

    }


    private void verifyEqualLoadBalance(final int iterations) {
        for (int i = 0; i < blobStores.size(); i++) {
            verify(blobStores.get(i), times(iterations / 4)).getContext();
        }
    }


    private void verifyFairLoadBalance(final int iterations, final int acceptedDiffrence) {
        for (int i = 0; i < blobStores.size(); i++) {
            verify(blobStores.get(i), atMost(iterations / 4 + acceptedDiffrence)).getContext();
            verify(blobStores.get(i), atLeast(iterations / 4 - acceptedDiffrence)).getContext();
        }
    }


    private void mockGetContextOperation() {
        for (int i = 0; i < blobStores.size(); i++) {
            when(blobStores.get(i).getContext()).thenReturn(mock(BlobStoreContext.class));

        }
    }
}
