package eu.europeana.cloud.service.ics.rest.rest_java_client;

import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.ics.converter.exceptions.ICSException;
import eu.europeana.cloud.service.ics.converter.exceptions.UnexpectedExtensionsException;
import eu.europeana.cloud.service.ics.rest.data.FileInputParameter;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Created by Tarek on 8/24/2015.
 */

/**
 * Exposes API related to converter resource.
 */
public class ICSServiceClient {
    private final Client client;
    private final String baseUrl;

    public ICSServiceClient(String baseUrl) {
        client = JerseyClientBuilder.newClient().register(MultiPartFeature.class);
        this.baseUrl = baseUrl;

    }
    /**
     *  converts an image with a format to the same image with a different format
     *
     * @param fileInputParameter The input parameter which should have variables needed to specify the input/output file and the properties of the conversion
     * @return url for the newly created file
     * @throws RepresentationNotExistsException when representation does not exist in specified version.
     * @throws FileNotExistsException           when requested file does not exist.
     * @throws DriverException                  call to service has not succeeded because of server side error.
     * @throws MCSException                     on unexpected situations.
     * @throws ICSException                     on unexpected situations.
     * @throws IOException
     */
    public String convertFile(FileInputParameter fileInputParameter) throws RepresentationNotExistsException, FileNotExistsException,UnexpectedExtensionsException,IOException, DriverException, MCSException, ICSException {
        String uri = null;
        WebTarget webTarget = client.target(baseUrl);

        Response response = webTarget.
                request().accept("application/json").put(Entity.json(fileInputParameter));

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            uri = response.readEntity(String.class);
        }

        return uri;

    }

}
