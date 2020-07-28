package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

//import org.apache.storm.kafka.*;


/**
 * @deprecated Use rather {@link ECloudSpout}
 * Created by Tarek on 11/27/2017.
 */
@Deprecated
public class CustomKafkaSpout extends KafkaSpout {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaSpout.class);

    protected static volatile TaskStatusChecker taskStatusChecker;

    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;
    protected transient TaskStatusUpdater taskStatusUpdater;

    protected CustomKafkaSpout(KafkaSpoutConfig spoutConf) {
        super(spoutConf);
    }

    public CustomKafkaSpout(KafkaSpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                            String userName, String password) {
        super(spoutConf);
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        LOGGER.info("Custom spout opened");
        super.open(conf, context, collector);
        CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        taskStatusUpdater = TaskStatusUpdater.getInstance(cassandraConnectionProvider);
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
}
