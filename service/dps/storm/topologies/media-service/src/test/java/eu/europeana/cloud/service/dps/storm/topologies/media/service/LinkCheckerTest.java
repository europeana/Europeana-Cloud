package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.client.ClientProtocolException;
import org.junit.Rule;
import org.junit.Test;

import java.net.UnknownHostException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;

public class LinkCheckerTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));

    @Test
    public void shouldWorkForHttpConnection() throws Exception {
        LinkChecker linkChecker = new LinkChecker();
        int status = linkChecker.check("http://www.man.poznan.pl");
        assertEquals(200, status);
    }

    @Test
    public void shouldWorkForHttpsConnection() throws Exception {
        LinkChecker linkChecker = new LinkChecker();
        int status = linkChecker.check("https://www.onet.pl");
        assertEquals(200, status);
    }

    @Test
    public void shouldWorkForRejectedHeadConnection() throws Exception {
        stubFor(head(urlEqualTo("/sample"))
                .willReturn(aResponse()
                        .withStatus(401)));

        stubFor(get(urlEqualTo("/sample"))
                .willReturn(aResponse()
                        .withStatus(200)));

        LinkChecker linkChecker = new LinkChecker();
        int status = linkChecker.check("http://localhost:9999/sample");
        assertEquals(200, status);
    }

    @Test
    public void shouldReturnCorrectStatusForProperHead200Response() throws Exception {
        stubFor(head(urlEqualTo("/sample"))
                .willReturn(aResponse()
                        .withStatus(200)));

        LinkChecker linkChecker = new LinkChecker();
        int status = linkChecker.check("http://localhost:9999/sample");
        assertEquals(200, status);
    }

    @Test
    public void shouldReturnCorrectStatusForProperHead204Response() throws Exception {
        stubFor(head(urlEqualTo("/sample"))
                .willReturn(aResponse()
                        .withStatus(204)));

        LinkChecker linkChecker = new LinkChecker();
        int status = linkChecker.check("http://localhost:9999/sample");
        assertEquals(204, status);
    }

    @Test
    public void shouldReturnCorrectStatusForProperHead206Response() throws Exception {
        stubFor(head(urlEqualTo("/sample"))
                .willReturn(aResponse()
                        .withStatus(206)));

        LinkChecker linkChecker = new LinkChecker();
        int status = linkChecker.check("http://localhost:9999/sample");
        assertEquals(206, status);
    }

    @Test
    public void shouldReturnCorrectStatusForProperHead404Response() throws Exception {
        stubFor(head(urlEqualTo("/sample"))
                .willReturn(aResponse()
                        .withStatus(404)));

        LinkChecker linkChecker = new LinkChecker();
        int status = linkChecker.check("http://localhost:9999/sample");
        assertEquals(404, status);
    }

    @Test(expected = ClientProtocolException.class)
    public void shouldThrowExceptionForMalformedURI() throws Exception {
        LinkChecker linkChecker = new LinkChecker();
        linkChecker.check("nonExistingURI");
    }

    @Test(expected = UnknownHostException.class)
    public void shouldThrowExceptionForNotExistingURI() throws Exception {
        LinkChecker linkChecker = new LinkChecker();
        linkChecker.check("http://www.notExistingURIToSomeResource.europeana");
    }
}