package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.dspace.xoai.serviceprovider.exceptions.HttpException;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;


/**
 * Created by Tarek on 3/7/2019.
 */
public class CustomConnection {
    private String baseUrl;
    private CloseableHttpClient httpclient;
    private int timeout = '\uea60';
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomConnection.class);

    public CustomConnection(String baseUrl) {
        this.baseUrl = baseUrl;
        this.httpclient = HttpClientBuilder.create().setConnectionTimeToLive(timeout, TimeUnit.SECONDS).build();
    }

    public String execute(Parameters parameters) throws HttpException {
        HttpGet httpGet = null;
        try {
            httpGet = this.createGetRequest(parameters);
            HttpResponse response = this.httpclient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == 200) {
                return getData(response.getEntity().getContent());
            } else {
                throw new HttpException("Error querying service. Returned HTTP Status Code: " + response.getStatusLine().getStatusCode());
            }
        } catch (IOException exception) {
            throw new HttpException(exception);
        } finally {
            try {
                if (httpGet != null)
                    httpGet.releaseConnection();
            } finally {
                if (httpclient != null)
                    try {
                        httpclient.close();
                    } catch (Exception e) {
                        LOGGER.error("Error while closing the stream {}. Because of {}", e.getMessage(), e.getCause());
                    }
            }
        }
    }


    private String getData(InputStream is) throws IOException {
        try {
            return IOUtils.toString(is, "UTF-8");
        } finally {
            if (is != null)
                is.close();
        }
    }

    private HttpGet createGetRequest(Parameters parameters) {
        return new HttpGet(parameters.toUrl(this.baseUrl));
    }
}