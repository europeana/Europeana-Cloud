package eu.europeana.cloud.service.dps.oaipmh;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_XML;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;

/**
 * @author krystian.
 */
public abstract class WiremockHelper {

    protected static final int DEFAULT_RETRIES = 3;
    protected static final int SLEEP_TIME = 5000;

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

    public static InputStream getFileContentAsStream(String name) {
        return Object.class.getResourceAsStream(name);
    }

    public static ResponseDefinitionBuilder responsTimeoutGreaterThanSocketTimeout(String fileContent, int timeout) {
        return  aResponse()
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withStatus(200)
                .withBody(fileContent)
                .withFixedDelay((int)(1.1*(double)timeout));
    }
}
