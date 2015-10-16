package eu.europeana.cloud.service.ips;

import eu.europeana.cloud.common.utils.FileUtils;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
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

    @Autowired
    public ImageTranslator(@Value("") String iipHost) {
        this.iipImageServer = iipHost;
    }

    public String getResponse(String globalId, String schema, String version, String fileName) {

        // prepare file name of image file
        String imageName = FileUtils.generateKeyForFile(globalId, schema, version, fileName);

        // Create an instance of HttpClient.
        HttpClient client = new HttpClient();

        String url = iipImageServer + "?IIIF=" + imageName + "/info.json";

        // Create a method instance.
        GetMethod method = new GetMethod(url);

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        try {
            // Execute the method.
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Getting manifest for " + imageName + " failed: " + method.getStatusLine());
            }

            // Read the response body.
            byte[] responseBody = method.getResponseBody();

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            return new String(responseBody);

        } catch (HttpException e) {
            System.err.println("Fatal protocol violation: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Fatal transport error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
        return null;
    }
}
