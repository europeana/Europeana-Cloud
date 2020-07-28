package eu.europeana.cloud.service.dps.service.kafka.util;

import eu.europeana.cloud.service.dps.DpsRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class DpsRecordDeserializer implements Deserializer<DpsRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DpsRecordDeserializer.class);

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        //nothing to implement
    }

    @Override
    public DpsRecord deserialize(String s, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(bytes, DpsRecord.class);
        } catch (IOException e) {
            LOGGER.error("Exception happened because of {} for the object {}", e.getMessage(), new String(bytes));
        }
        return null;
    }

    @Override
    public void close() {
        //nothing to implement
    }
}
