package eu.europeana.cloud.service.dls;

import java.util.ArrayList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.List;

public class TestUtil {

    public static <T> void assertSameContent(Collection<? extends T> actual, Collection<? extends T> expected) {
        List<T> actualSet = new ArrayList<T>(actual);
        List<T> expectedSet = new ArrayList<>(expected);
        assertThat(actual.size(), is(expected.size()));
        assertThat(actualSet, is(expectedSet));
    }
}
