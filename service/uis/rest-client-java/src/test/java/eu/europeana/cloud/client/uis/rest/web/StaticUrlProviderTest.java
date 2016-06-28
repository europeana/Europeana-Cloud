package eu.europeana.cloud.client.uis.rest.web;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;


@RunWith(JUnitParamsRunner.class)
public class StaticUrlProviderTest {

    public static final String URL_PREFIX = "http://localhost:8080/";

    @Test
    @Parameters({"uis/,uis","uis,uis","uis//,uis/"})
    public void shouldGetUrlWithoutSlashAtTheEnd(String inputSuffix, String expectedSuffix) {
        //given
        StaticUrlProvider provider = new StaticUrlProvider(URL_PREFIX + inputSuffix);
        //when
        String result = provider.getBaseUrl();
        //then
        assertThat(result,is(URL_PREFIX + expectedSuffix));
    }
}