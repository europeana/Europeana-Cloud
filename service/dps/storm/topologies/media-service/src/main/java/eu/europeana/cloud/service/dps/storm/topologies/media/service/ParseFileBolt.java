package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.google.gson.Gson;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;

import eu.europeana.metis.mediaprocessing.*;

/**
 * Created by Tarek on 12/6/2018.
 */
public class ParseFileBolt extends ReadFileBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseFileBolt.class);
    private Gson gson;
    private RdfDeserializer rdfDeserializer;

    public ParseFileBolt(String ecloudMcsAddress) {
        super(ecloudMcsAddress);
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        try (InputStream stream = getFileStreamByStormTuple(stormTaskTuple)) {
            byte[] fileContent = IOUtils.toByteArray(stream);
            List<RdfResourceEntry> rdfResourceEntries = rdfDeserializer.getResourceEntriesForMediaExtraction(fileContent);
            if (rdfResourceEntries.size() == 0) {
                StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
                LOGGER.info("The EDM file has no resource Links ");
                tuple.setFileData(fileContent);
                outputCollector.emit(tuple.toStormTuple());
            } else {
                for (RdfResourceEntry rdfResourceEntry : rdfResourceEntries) {
                    if (taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId()))
                        break;
                    StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
                    LOGGER.info("Sending this resource link {} to be processed ", rdfResourceEntry.getResourceUrl());
                    tuple.setFileData(fileContent);
                    tuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, gson.toJson(rdfResourceEntry));
                    tuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(rdfResourceEntries.size()));
                    outputCollector.emit(tuple.toStormTuple());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unable to read and parse file ", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while reading and parsing the EDM file. The full error is: " + ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    public void prepare() {
        super.prepare();
        try {
            rdfDeserializer = new RdfConverterFactory().createRdfDeserializer();
            gson = new Gson();
        } catch (Exception e) {
            LOGGER.error("Unable to initialize RDF Deserializer ", e);
            throw new RuntimeException(e);
        }
    }
}
