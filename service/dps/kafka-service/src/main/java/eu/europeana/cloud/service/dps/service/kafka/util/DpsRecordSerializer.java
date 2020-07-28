package eu.europeana.cloud.service.dps.service.kafka.util;

import eu.europeana.cloud.service.dps.DpsRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * This class is used inside {@link eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService}
 * to serialize tasks to Kafka
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
        try {
            return objectMapper.writeValueAsString(dpsRecord).getBytes();
        } catch (JsonProcessingException e) {
            LOGGER.error(String.format("Json processing failed for object: %s", dpsRecord.getClass().getName()), e);
        } catch (IOException e) {
            LOGGER.error("Exception happened because of {} for the object {} ", e.getMessage(), dpsRecord.getClass().getName());
        }
        return "".getBytes();
    }

    @Override
    public void close() {
        //nothing to implement
    }
}