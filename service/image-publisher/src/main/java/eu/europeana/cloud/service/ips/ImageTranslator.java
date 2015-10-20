package eu.europeana.cloud.service.ips;

import eu.europeana.cloud.common.utils.FileUtils;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Created by helin on 2015-10-14.
 */
@Component
public class ImageTranslator {

    // URL to IIP Image Server
    private String iipImageServer = null;

    static final private String IIIF_PARAMETERS = "?IIIF=$1/info.json";

    private static final Logger LOGGER = LoggerFactory.getLogger(ImageTranslator.class);

    @Autowired
    public ImageTranslator(@Value("") String iipHost) {
        this.iipImageServer = iipHost;
    }

    /**
     * Returns response from IIP Image Server according to the filename created with specified parameters
     *
     * @param globalId cloud identifier
     * @param schema representation name
     * @param version version name
     * @param fileName file name
     * @return manifest file in json format
     */
    public String getResponse(String globalId, String schema, String version, String fileName) {

        if (iipImageServer == null || iipImageServer.isEmpty()) {
            LOGGER.warn("Image server not available.");
            return null;
        }

        // prepare file name of image file
        String imageName = FileUtils.generateKeyForFile(globalId, schema, version, fileName);
        // when any part of name is missing there is no sense trying to get manifest file
        if (imageName == null)
            return null;

        // Create an instance of HttpClient.
        HttpClient client = new HttpClient();

        // Create a method instance.
        GetMethod method = new GetMethod(prepareURL(imageName));

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        try {
            // Execute the method.
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                LOGGER.error("Getting manifest for " + imageName + " failed: " + method.getStatusLine());
            }

            // Read the response body.
            byte[] responseBody = method.getResponseBody();

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            return new String(responseBody);

        } catch (HttpException e) {
            LOGGER.error("Fatal protocol violation: " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Fatal transport error: " + e.getMessage());
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
        return null;
    }

    private String prepareURL(String imageName) {
        return iipImageServer + IIIF_PARAMETERS.replace("$1", imageName);
    }
}
