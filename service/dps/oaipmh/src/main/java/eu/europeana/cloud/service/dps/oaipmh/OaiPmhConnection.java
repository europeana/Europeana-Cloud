package eu.europeana.cloud.service.dps.oaipmh;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.dspace.xoai.serviceprovider.exceptions.HttpException;
import org.dspace.xoai.serviceprovider.parameters.Parameters;

import java.io.IOException;
import java.io.InputStream;


/**
 * This is a connection that returns the content as a string. This connection can be executed
 * multiple times.
 *
 * Created by Tarek on 3/7/2019.
 */
class OaiPmhConnection {

    private final String url;
    private final RequestConfig requestConfig;

    private static final int DEFAULT_REQUEST_TIMEOUT = 60*1000 /* = 1min */;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 30*1000 /* = 30sec */;
    private static final int DEFAULT_SOCKET_TIMEOUT = 5*60*1000 /* = 5min */;

    /**
     * Constructor for using with default request, connection and socket timeouts.
     *
     * @param oaiPmhEndpoint The base URL of the connection.
     * @param parameters The parameters of the connection.
     */
    OaiPmhConnection(String oaiPmhEndpoint, Parameters parameters) {
        this(oaiPmhEndpoint, parameters, DEFAULT_REQUEST_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT,
                DEFAULT_SOCKET_TIMEOUT);
    }

    /**
     * Constructor.
     *
     * @param oaiPmhEndpoint The base URL of the connection.
     * @param parameters The parameters of the connection.
     * @param requestTimeout The request timeout in milliseconds.
     * @param connectionTimeout The connection timeout in milliseconds.
     * @param socketTimeout The socket timeout in milliseconds.
     */
    OaiPmhConnection(String oaiPmhEndpoint, Parameters parameters, int requestTimeout,
            int connectionTimeout, int socketTimeout) {
        this.url = parameters.toUrl(oaiPmhEndpoint);
        this.requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout)
                .setConnectionRequestTimeout(requestTimeout)
                .build();
    }

    /**
     * Executes the connection and returns the result.
     *
     * @return The result (body of the response) as a String.
     * @throws HttpException In case there was a connection issue.
     */
    String execute() throws HttpException {
        HttpGet httpGet = null;
        try (CloseableHttpClient httpclient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig).build()) {
            httpGet = new HttpGet(url);
            HttpResponse response = httpclient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return getData(response.getEntity());
            } else {
                throw new HttpException("Error querying service. Returned HTTP Status Code: " + response.getStatusLine().getStatusCode());
            }
        } catch (IOException exception) {
            throw new HttpException(exception);
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
    }

    private String getData(HttpEntity entity) throws IOException {
        try (final InputStream inputStream = entity.getContent()) {
            return IOUtils.toString(inputStream, "UTF-8");
        }
    }
}
