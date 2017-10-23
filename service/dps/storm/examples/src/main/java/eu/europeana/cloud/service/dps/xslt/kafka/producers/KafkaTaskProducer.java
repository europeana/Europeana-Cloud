package eu.europeana.cloud.service.dps.xslt.kafka.producers;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class KafkaTaskProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9093,localhost:9094");
        props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.kafka.JsonEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, DpsTask> producer = new Producer<String, DpsTask>(config);
        DpsTask dpsTask = new DpsTask();
        dpsTask.setTaskName("my task");
        HashMap<InputDataType, List<String>> inputData = new HashMap<>();
        List<String> taskList = new ArrayList<String>();
        taskList.add("an item of list");
        inputData.put(InputDataType.FILE_URLS, taskList);

        dpsTask.setInputData(inputData);
        KeyedMessage<String, DpsTask> data = new KeyedMessage<>("my_topic", "my key", dpsTask);
        producer.send(data);
        producer.close();
    }
}
