package eu.europeana.cloud.service.dps.oaipmh;

import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.dspace.xoai.serviceprovider.client.OAIClient;
import org.dspace.xoai.serviceprovider.exceptions.HttpException;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @see OAIClient
 * @see org.dspace.xoai.serviceprovider.client.HttpOAIClient
 *
 */
public class HarvesterHttpOAIClient implements OAIClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HarvesterImpl.class);

    private static final int TIMEOUT = 60 * 1000;

    private String baseUrl;
    private CloseableHttpClient httpClient;
    private CloseableHttpResponse response = null;

    public HarvesterHttpOAIClient(String baseUrl) {
        this.baseUrl = baseUrl;

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(TIMEOUT)
                .setSocketTimeout(TIMEOUT)
                .build();

        httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Override
    public InputStream execute(Parameters parameters) throws HttpException {
        try {
            response = httpClient.execute(createGetRequest(parameters));

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return response.getEntity().getContent();
            } else {
                response.close();
                throw new HttpException(
                        "Error querying service. Returned HTTP Status Code: "
                                + response.getStatusLine().getStatusCode());
            }
        } catch (IOException e) {
            throw new HttpException(e);
        }
    }

    private HttpUriRequest createGetRequest(Parameters parameters) {
        return new HttpGet(parameters.toUrl(baseUrl));
    }

    public void closeResponse() {
        if(response != null) {
            try {
                response.close();
            } catch (IOException e) {
                LOGGER.warn("Error while closing reponse", e);
            }
        }
    }

}

