package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static java.lang.Integer.parseInt;

/**
 * Created by Tarek on 11/27/2017.
 */
public class CustomKafkaSpout extends KafkaSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaSpout.class);
    protected static volatile TaskStatusChecker taskStatusChecker;

    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;
    protected CassandraTaskInfoDAO cassandraTaskInfoDAO;

    protected CustomKafkaSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    public CustomKafkaSpout(SpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                            String userName, String password) {

        super(spoutConf);
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;

    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        LOGGER.info("Custom spout opened");
        super.open(conf, context, collector);
        CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        cassandraTaskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        synchronized (CustomKafkaSpout.class) {
            if (taskStatusChecker == null) {
                try {
                    TaskStatusChecker.init(cassandraConnectionProvider);
                } catch (IllegalStateException e) {
                    LOGGER.info("It was already initialized");
                }
                taskStatusChecker = TaskStatusChecker.getTaskStatusChecker();
            }
        }
    }


    @Override
    public void ack(Object msgId) {
        super.ack(msgId);

    }

    @Override
    public void fail(Object msgId) {
        super.ack(msgId);

    }
}
