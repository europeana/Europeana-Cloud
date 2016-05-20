package eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api.ImageConverterService;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api.ImageConverterServiceImpl;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.tika.mime.MimeTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Tarek on 9/28/2015.
 */

/**
 * compress image file from tiff to jp2 using its url and emits the file as separate {@link StormTaskTuple}.
 */
public class IcBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(IcBolt.class);


    /**
     * compress image file.
     *
     * @param stormTaskTuple Tuple which DpsTask is part of ...
     */

    public void execute(StormTaskTuple stormTaskTuple) {
        try {
            String fileUrl = stormTaskTuple.getFileUrl();
            LOGGER.info("processing file: {}", fileUrl);
            ImageConverterService imageConverterService = new ImageConverterServiceImpl();
            imageConverterService.convertFile(stormTaskTuple);
            LOGGER.info("IC Bolt: conversion success for: {}", fileUrl);
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } catch (IOException | MimeTypeException | MCSException | ICSException | RuntimeException e) {
            LOGGER.error("IC Bolt error: {} \n StackTrace: \n{}", e.getMessage(), e.getStackTrace());
            logAndEmitError(stormTaskTuple, e.getMessage());
        }
    }

    @Override
    public void prepare() {
    }
}
