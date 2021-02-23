package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsRecordDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HungryConsumerTester {

    //private final String SERVERS = "127.0.0.1:9092";
    private final String SERVERS = "mandevilla-dev.man.poznan.pl:9092,dipladenia-dev.man.poznan.pl:9092";
    private KafkaConsumer<String, DpsRecord> consumer;

    public HungryConsumerTester(){
        consumer=createConsumer();
    }

    public KafkaConsumer<String, DpsRecord> createConsumer() {
        Map<String,Object> props=new HashMap<>();
        props.put("key.deserializer",  org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put("value.deserializer", DpsRecordDeserializer.class);
        props.put("enable.auto.commit", false);
        props.put("enable.auto.commit", true);
        props.put("max.poll.records", 100);
   //     props.put("fetch.max.bytes", 3000);
        //props.put("group.id" , "consumer_tester");
        props.put("group.id" , "spout_testing_topology");

        props.put("bootstrap.servers" , SERVERS);
        props.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(props);
    }

    public static void main(String[] args) {
        HungryConsumerTester tester = new HungryConsumerTester();
        tester.consume();
    }

    private void consume() {
        int [] counters=new int[5];
        assign();
        while(true) {
            ConsumerRecords<String, DpsRecord> polled = consumer.poll(5000L);


            for(ConsumerRecord<String, DpsRecord> record:polled){
                //System.out.println(record);
                counters[Integer.valueOf(record.value().getMetadataPrefix())-1]++;



            }
            int c1 = counters[0];
            int c2 = counters[1];
            double fairness=100.0-Math.abs(c1-c2)*100.0/(c1+c2);
            System.out.println("POLLED: "+polled.count()+" fairness: "+(int)fairness+ " diff: " + c1 +" / " +c2);

        }

    }

    private void assign() {
        List<PartitionInfo> set = new ArrayList<>();
        set.addAll(consumer.partitionsFor("fair_1"));
        set.addAll(consumer.partitionsFor("fair_2"));
        set.addAll(consumer.partitionsFor("fair_3"));
        set.addAll(consumer.partitionsFor("fair_4"));
        set.addAll(consumer.partitionsFor("fair_5"));
        consumer.assign(set.stream().map(info -> new TopicPartition(info.topic(), info.partition())).collect(Collectors.toList()));

    }
}
