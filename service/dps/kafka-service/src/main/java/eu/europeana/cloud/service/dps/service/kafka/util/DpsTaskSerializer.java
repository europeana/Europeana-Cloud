package eu.europeana.cloud.service.dps.service.kafka.util;

import eu.europeana.cloud.service.dps.DpsTask;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/*
This class is used inside {@link eu.europeana.cloud.service.dps.service.kafka.RecordKafkaSubmitService}  to serialize tasks to Kafka
 */
public class DpsTaskSerializer implements Serializer<DpsTask> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DpsTaskSerializer.class);

    @Override
    public void configure(Map<String, ?> parameters, boolean b) {
    }

    @Override
    public byte[] serialize(String s, DpsTask dpsTask) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(dpsTask).getBytes();
        } catch (JsonProcessingException e) {
            LOGGER.error(String.format("Json processing failed for object: %s", dpsTask.getClass().getName()), e);
        } catch (IOException e) {
            LOGGER.error("Exception happened because of {} for the object {} ", e.getMessage(), dpsTask.getClass().getName());
        }
        return "".getBytes();
    }

    @Override
    public void close() {
    }
}