package eu.europeana.cloud.service.mcs.persistent.swift;

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.Before;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RandomRingIteratorTest {

    RandomIterator<StringHolder> iterator;
    private List<StringHolder> strings;


    private class StringHolder {

        String inside;


        public StringHolder(String inside) {
            this.inside = inside;
        }


        public String toStringCustom() {
            return "StringHolder{" + "inside=" + inside + '}';
        }
    }


    @Before
    public void init() {
        strings = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            strings.add(spy(new StringHolder(String.valueOf(i))));
        }
        iterator = new RandomIterator<>(Iterators.cycle(strings), 0.5);
    }


    @Test
    public void shouldDistibuteLoadUniformWithMultiRetries() {
        //given
        int iterations = 500;
        double maxDiffrend = 0.10; //relative to number of iterations
        //when

        for (int i = 0; i < iterations; i++) {
            iterator.next().toStringCustom();
        }
        //then
        verifyFairLoadBalance(iterations, (int) (iterations * maxDiffrend));

    }


    private void verifyFairLoadBalance(final int iterations, final int acceptedDiffrence) {
        for (int i = 0; i < strings.size(); i++) {
            verify(strings.get(i), atMost(iterations / strings.size() + acceptedDiffrence)).toStringCustom();
            verify(strings.get(i), atLeast(iterations / strings.size() - acceptedDiffrence)).toStringCustom();
        }
    }

}
