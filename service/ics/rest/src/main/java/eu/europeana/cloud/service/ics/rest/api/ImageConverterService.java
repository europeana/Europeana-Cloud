package eu.europeana.cloud.service.ics.rest.api;

import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.ics.converter.exceptions.ICSException;
import eu.europeana.cloud.service.ics.converter.exceptions.UnexpectedExtensionsException;
import eu.europeana.cloud.service.ics.rest.data.FileInputParameter;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

import java.io.IOException;

/**
 * Service for converting an image with a format to the same image with a different format
 */
public interface ImageConverterService {
    /**
     * Converts image file with a format to the same image with different format
     *
     * @param fileInputParameter The input parameter which should have variables needed to specify the input/output file and the properties of the conversion
     * @return path for the newly created file
     * @throws RepresentationNotExistsException when representation does not exist in specified version.
     * @throws FileNotExistsException           when requested file does not exist.
     * @throws DriverException                  call to service has not succeeded because of server side error.
     * @throws MCSException                     on unexpected situations.
     * @throws ICSException                     on unexpected situations.
     * @throws IOException
     */
    public String convertFile(FileInputParameter fileInputParameter) throws RepresentationNotExistsException, UnexpectedExtensionsException, FileNotExistsException, IOException, DriverException, MCSException, ICSException;
}

