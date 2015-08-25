package eu.europeana.cloud.service.dps.storm;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.service.cassandra.CassandraTablesAndColumnsNames;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationBolt extends BaseBasicBolt
{
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
    
    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    
    private String topologyName;
            
    private CassandraConnectionProvider cassandra;
    
    
    public NotificationBolt(String hosts, int port, String keyspaceName, String userName, String password) 
    {
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
    }
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) 
    {
        NotificationTuple notificationTuple = NotificationTuple.fromStormTuple(tuple);
        
        try
        {
            switch(notificationTuple.getInformationType())
            {
                case BASIC_INFO:
                    storeBasicInfo(notificationTuple.getTaskId(), notificationTuple.getParameters());
                    break;
                case NOTIFICATION:
                    storeNotification(notificationTuple.getTaskId(), notificationTuple.getParameters());
                    break;
            }
        }
        catch(Exception ex)
        {
            LOGGER.error("Cannot store notifiaction to Cassandra because: {}", ex.getMessage());
            return;
        }
        
        boc.emit(notificationTuple.toStormTuple());
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) 
    {
        cassandra = new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        
        super.prepare(stormConf, context);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) 
    {
        ofd.declare(NotificationTuple.getFields());
    }   
    
    private void storeBasicInfo(long taskId, Map<String, String> parameters)
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
}
