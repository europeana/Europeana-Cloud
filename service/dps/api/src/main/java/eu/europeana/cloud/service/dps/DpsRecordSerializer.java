package eu.europeana.cloud.service.dps;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Serializer for {@link DpsRecord} used while writing/reading records to/from Kafka.
 */
public class DpsRecordSerializer implements Serializer<DpsRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DpsRecordSerializer.class);

    @Override
    public void configure(Map<String, ?> parameters, boolean b) {
        //nothing to implement
    }

    @Override
    public byte[] serialize(String s, DpsRecord dpsRecord) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        try {
            return objectMapper.writeValueAsString(dpsRecord).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            LOGGER.error(String.format("Json processing failed for object: %s", dpsRecord.getClass().getName()), e);
        } catch (IOException e) {
            LOGGER.error("Exception happened because of {} for the object {} ", e.getMessage(), dpsRecord.getClass().getName());
        }
        return "".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        //nothing to implement
    }
}