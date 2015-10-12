package eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api;

import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
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
     * @param stormTaskTuple  Tuple which DpsTask is part of ...
     * @return path for the newly created file
     * @throws MCSException                     on unexpected situations.
     * @throws ICSException
     * @throws IOException
     */
    public void convertFile(StormTaskTuple stormTaskTuple) throws   IOException, MCSException, ICSException;
}

