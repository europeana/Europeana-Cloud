package eu.europeana.cloud.service.mcs.persistent;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TestUtil {

    public static <T> void assertSameContent(Collection<? extends T> actual, Collection<? extends T> expected) {
        Set<T> actualSet = new HashSet<>(actual);
        Set<T> expectedSet = new HashSet<>(expected);
        assertThat(actualSet, is(expectedSet));
    }
}
