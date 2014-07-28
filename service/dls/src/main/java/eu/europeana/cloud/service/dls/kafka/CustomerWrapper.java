package eu.europeana.cloud.service.dls.kafka;

import com.github.ddth.kafka.KafkaConsumer;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * CustomerWrapper create message listener message from Kafka broker.
 * 
 */
public class CustomerWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerWrapper.class);

    private static final String GROUP_ID = "ecloud";

    private final String topic;

    private final KafkaConsumer consumer;

    @Autowired(required = true)
    private KafkaMessageListener messageListener;


    /**
     * Constructor method.
     * 
     * @param zookeeperList
     *            list of zookeepers managing kafka brokers instances in format "host1:port,host2:port,host3:port" or
     *            "host1:port,host2:port,host3:port/chroot"
     * @param topic
     *            topic from which messages are received
     */
    public CustomerWrapper(String zookeeperList, String topic) {
        consumer = new KafkaConsumer(zookeeperList, GROUP_ID);
        this.topic = topic;

    }


    /**
     * Initialize method.
     */
    @PostConstruct
    public void init() {
        consumer.init();
        consumer.addMessageListener(topic, messageListener);
    }


    /**
     * Destroying method.
     */
    @PreDestroy
    public void destroy() {
        consumer.destroy();
    }

}
