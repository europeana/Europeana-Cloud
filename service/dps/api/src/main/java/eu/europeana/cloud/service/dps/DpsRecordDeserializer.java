package eu.europeana.cloud.service.dps;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * This class is used by Kafka consumer (e.g. in topologies spouts)
 * to convert serialized DpsRecord from array of bytes to DpsRecord object
 */
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
            LOGGER.error("Exception happened because of {} for the object {}", e.getMessage(), new String(bytes, StandardCharsets.UTF_8));
        }
        return null;
    }

    @Override
    public void close() {
        //nothing to implement
    }
}
