package eu.europeana.cloud.client.uis.rest.web;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


class StaticUrlProviderTest {

    public static final String URL_PREFIX = "http://localhost:8080/";

    @ParameterizedTest
    @CsvSource({"uis/,uis", "uis,uis", "uis//,uis/"})
    public void shouldGetUrlWithoutSlashAtTheEnd(String inputSuffix, String expectedSuffix) {
        //given
        StaticUrlProvider provider = new StaticUrlProvider(URL_PREFIX + inputSuffix);
        //when
        String result = provider.getBaseUrl();
        //then
        assertThat(result, is(URL_PREFIX + expectedSuffix));
    }
}