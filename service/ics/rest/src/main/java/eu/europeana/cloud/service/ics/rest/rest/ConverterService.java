package eu.europeana.cloud.service.ics.rest.rest;

import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.ics.converter.exceptions.ICSException;
import eu.europeana.cloud.service.ics.converter.exceptions.UnexpectedExtensionsException;
import eu.europeana.cloud.service.ics.rest.api.ImageConverterService;
import eu.europeana.cloud.service.ics.rest.data.FileInputParameter;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Created by Tarek on 8/13/2015.
 */


/**
 * Resource to manage the image conversion process
 */
@Path("/converter")
public class ConverterService {

    @Autowired
    private ImageConverterService imageConverterService;

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/json/single/file")

    /**
     *  converts an image with a format to the same image with a different format
     *
     *@param fileInputParameter The input parameter which should have variables needed to specify the input/output file and the properties of the conversion
     * @return url for the newly created file
     * @throws RepresentationNotExistsException when representation does not exist in specified version.
     * @throws FileNotExistsException           when requested file does not exist.
     * @throws DriverException                  call to service has not succeeded because of server side error.
     * @throws MCSException                     on unexpected situations.
     * @throws ICSException                     on unexpected situations.
     * @throws IOException
     */
    public Response convertFile(FileInputParameter fileInputParameter) throws RepresentationNotExistsException,UnexpectedExtensionsException,IOException, FileNotExistsException, DriverException, MCSException, ICSException {
        Response.Status status = Response.Status.OK;
        String url = imageConverterService.convertFile(fileInputParameter);
        return Response.status(status).entity(url).build();


    }
}

