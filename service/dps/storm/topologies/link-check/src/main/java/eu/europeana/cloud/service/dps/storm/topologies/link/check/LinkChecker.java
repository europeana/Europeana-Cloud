package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Checks if the given link (URL) is accessible (response status in 200)
 * <p>
 * Created by pwozniak on 2/6/19
 */
public class LinkChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinkChecker.class);
    private CloseableHttpClient httpclient;
    private RequestConfig requestConfig;

    private static final int CONNECTION_TIMEOUT = 2000;
    private static final int SOCKET_TIMEOUT = 5000;

    public LinkChecker() {
        initHttpClient();
    }

    public int check(String linkToBeChecked) throws Exception {
        LOGGER.info("Checking link: {}", linkToBeChecked);
        int status = sendHeadRequest(linkToBeChecked);
        if (isHeadRejected(status)) {
            LOGGER.info("HEAD rejected ({}), retrying with GET: {}", status, linkToBeChecked);
            status = sendGetRequest(linkToBeChecked);
        }

        LOGGER.info("Link status code {} for {}", status, linkToBeChecked);
        return status;
    }

    public void close() {
        try {
            this.httpclient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initHttpClient() {
        httpclient = HttpClients.createDefault();
        requestConfig = RequestConfig.custom()
                .setMaxRedirects(3)
                .setConnectTimeout(CONNECTION_TIMEOUT)
                .setSocketTimeout(SOCKET_TIMEOUT)
                .build();
    }

    private int sendHeadRequest(String linkToBeChecked) throws IOException {
        HttpHead httpHead = new HttpHead(linkToBeChecked);
        return execute(httpHead);
    }

    private int sendGetRequest(String linkToBeChecked) throws IOException {
        HttpGet request = new HttpGet(linkToBeChecked);
        return execute(request);
    }

    private int execute(HttpRequestBase httpRequestBase) throws IOException {
        httpRequestBase.setConfig(requestConfig);
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(httpRequestBase);
            return response.getStatusLine().getStatusCode();
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    private boolean isHeadRejected(int status) {
        return status >= 400 && status < 500;
    }
}
