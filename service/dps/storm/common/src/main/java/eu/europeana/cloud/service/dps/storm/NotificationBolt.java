package eu.europeana.cloud.service.dps.storm;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.service.cassandra.CassandraTablesAndColumnsNames;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bolt is responsible for store notifications to Cassandra.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationBolt extends BaseRichBolt
{
    public static final String TaskFinishedStreamName = "FinishStream";
    
    private static final String BASIC_INFO_TABLE = CassandraTablesAndColumnsNames.BASIC_INFO_TABLE;
    private static final String NOTIFICATIONS_TABLE = CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE;

    private static final String[] BASIC_INFO_COLUMNS = new String[] {
        CassandraTablesAndColumnsNames.BASIC_TASK_ID,
        CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME,
        CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE
    };
    private static final String[] NOTIFICATIONS_COLUMNS = new String[] {
        CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID,
        CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME, 
        CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE,
        CassandraTablesAndColumnsNames.NOTIFICATION_STATE,
        CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT,
        CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS
    };
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationBolt.class);
    
    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;
    
    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    private final Boolean grouping;
    
    private String topologyName;
    private Map<Long, NotificationCache> cache;
            
    private CassandraConnectionProvider cassandra;
    
    /**
     * Constructor of notification bolt.
     * @param hosts Cassandra hosts separated by comma (e.g. localhost,192.168.47.129)
     * @param port Cassandra port
     * @param keyspaceName Cassandra keyspace name
     * @param userName Cassandra username
     * @param password Cassandra password
     */
    public NotificationBolt(String hosts, int port, String keyspaceName, String userName, String password) 
    {
        this(hosts, port, keyspaceName, userName, password, false);
    }
    
    /**
     * Constructor of notification bolt.
     * @param hosts Cassandra hosts separated by comma (e.g. localhost,192.168.47.129)
     * @param port Cassandra port
     * @param keyspaceName Cassandra keyspace name
     * @param userName Cassandra username
     * @param password Cassandra password
     * @param grouping this bolt is connected to topology by fields grouping
     *        If true: keep number of notifications in memory and emit notification when task is completed.
     */
    public NotificationBolt(String hosts, int port, String keyspaceName, String userName, String password, Boolean grouping) 
    {
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
        this.grouping = grouping;
    }
    
    @Override
    public void execute(Tuple tuple) 
    {
        NotificationTuple notificationTuple = NotificationTuple.fromStormTuple(tuple);
        
        NotificationCache nCache = null;
        if(grouping)
        {
            nCache = cache.get(notificationTuple.getTaskId());
            if(nCache == null)
            {
                nCache = new NotificationCache();
                cache.put(notificationTuple.getTaskId(), nCache);
            }
        }
        
        try
        {
            switch(notificationTuple.getInformationType())
            {
                case BASIC_INFO:
                    int tmp = storeBasicInfo(notificationTuple.getTaskId(), notificationTuple.getParameters());
                    if(nCache != null)
                    {
                        nCache.setTotalSize(tmp);
                    }
                    break;
                case NOTIFICATION:
                    storeNotification(notificationTuple.getTaskId(), notificationTuple.getParameters());
                    if(nCache != null)
                    {
                        nCache.inc();
                    }
                    break;
            }
        }
        catch(Exception ex)
        {
            LOGGER.error("Cannot store notifiaction to Cassandra because: {}", ex.getMessage());
            outputCollector.ack(tuple);
            return;
        }
        
        outputCollector.emit(notificationTuple.toStormTuple());
        
        //emit finish notification
        if(nCache != null && nCache.isComplete())
        {
            outputCollector.emit(TaskFinishedStreamName, 
                    NotificationTuple.prepareNotification(notificationTuple.getTaskId(), 
                            "", NotificationTuple.States.FINISHED, "", "")
                    .toStormTuple());
            
            cache.remove(notificationTuple.getTaskId());
        }
        
        outputCollector.ack(tuple);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector oc) 
    {
        cassandra = new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        
        if(grouping)
        {
            cache = new HashMap<>();
        }
        
        this.stormConfig = stormConf;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) 
    {
        ofd.declare(NotificationTuple.getFields());
        
        ofd.declareStream(TaskFinishedStreamName, NotificationTuple.getFields());
    }   
    
    private int storeBasicInfo(long taskId, Map<String, String> parameters)
    {
        int expectedSize = -1;
        if(parameters != null)
        {
            String tmp = parameters.get(NotificationParameterKeys.EXPECTED_SIZE);
            if(tmp != null && !tmp.isEmpty())
            {
                expectedSize = Integer.valueOf(tmp);
            }
        }
        
        Insert insert = QueryBuilder.insertInto(BASIC_INFO_TABLE).values(BASIC_INFO_COLUMNS, 
                new Object[] {taskId, topologyName, expectedSize});
        
        cassandra.getSession().execute(insert);
        
        return expectedSize;
    }
    
    private void storeNotification(long taskId, Map<String, String> parameters)
    {
        String tmp;
        
        String resource = "";
        String state = "";
        String infoText = "";
        String additionalInfo = "";

        if(parameters != null)
        {          
            tmp = parameters.get(NotificationParameterKeys.RESOURCE);
            if(tmp != null)
            {
               resource = tmp; 
            }
            tmp = parameters.get(NotificationParameterKeys.STATE);
            if(tmp != null)
            {
               state = tmp; 
            }
            tmp = parameters.get(NotificationParameterKeys.INFO_TEXT);
            if(tmp != null)
            {
               infoText = tmp; 
            }
            tmp = parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS);       
            if(tmp != null)
            {
               additionalInfo = tmp; 
            }
        }
        
        Insert insert = QueryBuilder.insertInto(NOTIFICATIONS_TABLE).values(NOTIFICATIONS_COLUMNS, 
                new Object[] {taskId, topologyName, resource, state, infoText, additionalInfo});
        
        cassandra.getSession().execute(insert);
    }
    
    private class NotificationCache
    {
        int totalSize = -1;
        int processed = 0;
        
        public void setTotalSize(int totalSize)
        {
            this.totalSize = totalSize;
        }
        
        public void inc()
        {
            processed++;
        }
        
        public Boolean isComplete()
        {
            return totalSize != -1 ? processed >= totalSize : false;
        }
    }
}
