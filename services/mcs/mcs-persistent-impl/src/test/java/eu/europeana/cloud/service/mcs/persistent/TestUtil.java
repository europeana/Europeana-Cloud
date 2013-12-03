package eu.europeana.cloud.service.mcs.persistent;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


public class TestUtil {

	public static <T> void assertSameContent(Collection<? extends T> actual, Collection<? extends T> expected) {
		Set<T> actualSet = new HashSet<>(actual);
		Set<T> expectedSet = new HashSet<>(expected);
		assertThat(actualSet, is(expectedSet));
	}
}
