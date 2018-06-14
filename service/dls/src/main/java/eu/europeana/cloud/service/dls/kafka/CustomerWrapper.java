package eu.europeana.cloud.service.dls.kafka;

import com.github.ddth.kafka.KafkaConsumer;
import com.yammer.metrics.Metrics;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * CustomerWrapper - wrapper for {@link com.github.ddth.kafka.KafkaConsumer} and
 * {@link eu.europeana.cloud.service.dls.kafka.KafkaMessageListener}.
 * 
 */
public class CustomerWrapper {


    private static final String GROUP_ID = "ecloud";

    private final String topic;

    private final KafkaConsumer consumer;

    @Autowired(required = true)
    private KafkaMessageListener messageListener;


    /**
     * Constructor method.
     * 
     * @param zookeeperList
     *            list of zookeepers managing kafka broker instances in format "host1:port,host2:port,host3:port" or
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
        Metrics.defaultRegistry().shutdown();
    }

}
