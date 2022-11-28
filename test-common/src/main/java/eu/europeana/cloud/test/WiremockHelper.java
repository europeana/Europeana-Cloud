package eu.europeana.cloud.test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.util.Map;

public class WiremockHelper {

  private static final String LOCATION_HEADER_NAME = "Location";
  private static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
  private static final String APPLICATION_XML = "application/xml";
  private static final String APPLICATION_JSON = "application/json";


  private final WireMockRule wireMockRule;

  public WiremockHelper(WireMockRule wireMockRule) {
    this.wireMockRule = wireMockRule;
  }


  public void reset() {
    wireMockRule.resetAll();
  }

  public void stubGet(String url, int responseStatus, String responseBody) {
    wireMockRule.stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_XML)
            .withBody(responseBody)));
  }

  public void stubGetWithJsonContent(String url, int responseStatus, String responseBody) {
    wireMockRule.stubFor(get(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_JSON)
            .withBody(responseBody)));
  }

  public void stubPost(String url, int responseStatus, Map<String, String> headers, String responseBody) {
    HttpHeaders httpHeaders = new HttpHeaders();
    for (var entry : headers.entrySet()) {
      httpHeaders.plus(HttpHeader.httpHeader(entry.getKey(), entry.getValue()));
    }
    wireMockRule.stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeaders(httpHeaders)
            .withBody(responseBody)));
  }

  public void stubPost(String url, int responseStatus, String responseBody) {
    wireMockRule.stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_XML)
            .withBody(responseBody)));
  }

  public void stubPost(String url, String requestBody, String locationHeader, int responseStatus, String responseBody) {
    wireMockRule.stubFor(post(urlEqualTo(url))
        .withRequestBody(matching(requestBody))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_XML)
            .withHeader(LOCATION_HEADER_NAME, locationHeader)
            .withBody(responseBody)));
  }

  public void stubPost(String url, JsonNode requestBody, int responseStatus, JsonNode responseBody) {
    wireMockRule.stubFor(post(urlEqualTo(url))
        .withRequestBody(requestBody != null ? containing(requestBody.toString()) : null)
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_JSON)
            .withJsonBody(responseBody)));
  }

  public void stubPost(String url, int responseStatus, String locationHeader, String eTag, String responseBody) {
    wireMockRule.stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_XML)
            .withHeader(LOCATION_HEADER_NAME, locationHeader)
            .withHeader("ETag", eTag)
            .withBody(responseBody)));
  }

  public void stubPost(String url, int responseStatus, String locationHeader, String responseBody) {
    wireMockRule.stubFor(post(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_XML)
            .withHeader(LOCATION_HEADER_NAME, locationHeader)
            .withBody(responseBody)));
  }

  public void stubDelete(String url, int responseStatus) {
    wireMockRule.stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)));
  }

  public void stubDelete(String url, int responseStatus, String responseBody) {
    wireMockRule.stubFor(delete(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_XML)
            .withBody(responseBody)));
  }

  public void stubPut(String url, int responseStatus) {
    wireMockRule.stubFor(put(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)));
  }

  public void stubPut(String url, int responseStatus, String locationHeader, String eTag, String responseBody) {
    wireMockRule.stubFor(put(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)
            .withHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_XML)
            .withHeader(LOCATION_HEADER_NAME, locationHeader)
            .withHeader("ETag", eTag)
            .withBody(responseBody)));
  }


  public void stubHead(String url, int responseStatus) {
    wireMockRule.stubFor(head(urlEqualTo(url))
        .willReturn(aResponse()
            .withStatus(responseStatus)));
  }
}
