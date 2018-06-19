package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;

import java.io.IOException;
import java.io.InputStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_XML;

/**
 * @author krystian.
 */
public abstract class WiremockHelper {
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8181));

    protected static ResponseDefinitionBuilder response200XmlContent(String fileContent) {
        return aResponse()
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withStatus(200)
                .withBody(fileContent);
    }

    protected static ResponseDefinitionBuilder response404() {
        return aResponse()
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withStatus(404);
    }

    public static String getFileContent(String name) throws IOException {
        return IOUtils.toString(
                Object.class.getResourceAsStream(name),
                "UTF-8");
    }

    public static InputStream getFileContentAsStream(String name) throws IOException {
        return Object.class.getResourceAsStream(name);
    }
}
