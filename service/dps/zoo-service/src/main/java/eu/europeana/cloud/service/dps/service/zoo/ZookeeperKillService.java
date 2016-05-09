package eu.europeana.cloud.service.dps.service.zoo;

import eu.europeana.cloud.service.coordination.ZookeeperService;
import eu.europeana.cloud.service.dps.TaskExecutionKillService;
import java.util.Date;
import java.util.List;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zookeeper implementation of {@link TaskExecutionKillService}.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ZookeeperKillService implements TaskExecutionKillService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperKillService.class);
    
    private final static int ZOOKEEPER_CONNECTION_TIME = 3000;
    private final static int ZOOKEEPER_SESSION_TIMEOUT = 3000;

    private final static String ZOOKEEPER_PATH = "/tasks-to-kill";
    
    private final ZookeeperService zS; 

    /**
     * Construct kill service via zookeeper.
     * @param zS instance of zookeeper service
     */
    public ZookeeperKillService(ZookeeperService zS) 
    {
        this.zS = zS;
    }
    
    /**
     * Construct kill service via zookeeper.
     * @param zookeeperConnectString zookeeper servers (e.g. localhost:2181,192.168.47.129:2181)
     */
    public ZookeeperKillService(String zookeeperConnectString)
    {
        zS = new ZookeeperService(zookeeperConnectString, 
                ZOOKEEPER_CONNECTION_TIME, ZOOKEEPER_SESSION_TIMEOUT, ZOOKEEPER_PATH);
    }
    
    @Override
    public void killTask(String topology, long taskId) 
    {
        String path = ZOOKEEPER_PATH +"/"+ topology +"/"+ String.valueOf(taskId);
        try
        {
            zS.getClient().create().creatingParentsIfNeeded().forPath(path);
        }
        catch(Exception ex) //node exists
        {
            LOGGER.warn("Cannot submit kill flag for task {} because: {}", taskId, ex.getMessage());
        }
    }    

    @Override
    public Boolean hasKillFlag(String topology, long taskId) 
    {
        String path = ZOOKEEPER_PATH +"/"+ topology +"/"+ String.valueOf(taskId);
        
        Stat stat = null;
        try
        {
            stat= zS.getClient().checkExists().forPath(path);
        }
        catch(Exception ex)
        {
            LOGGER.warn("Cannot check kill flag of task {} because: {}", taskId, ex.getMessage());
        }

        return stat != null;
    }

    @Override
    public void cleanOldFlags(String topology, long ttl) 
    {     
        String path = ZOOKEEPER_PATH +"/"+ topology;

        long threshold = new Date().getTime()-ttl;
        
        try
        {
            List<String> children = zS.getClient().getChildren().forPath(path);
            Stat stat;
            for(String child: children)
            {
                stat= zS.getClient().checkExists().forPath(path+"/"+child);
                if(stat != null)
                {
                    long lastModifiTime = stat.getMtime();    //time in milliseconds from epoch when this znode was last modified
                    if(lastModifiTime < threshold)
                    {
                        zS.getClient().delete().inBackground().forPath(path+"/"+child);
                    }
                }
            }
        }
        catch(Exception ex)
        {
            LOGGER.warn("Cannot clean kill flags of {} topology because: {}", topology, ex.getMessage());
        }
    }

    @Override
    public void removeFlag(String topology, long taskId) 
    {
        String path = ZOOKEEPER_PATH +"/"+ topology +"/"+ String.valueOf(taskId);
        try
        {
            zS.getClient().delete().inBackground().forPath(path);
        }
        catch(Exception ex)
        {
            LOGGER.warn("Cannot remove kill flag of {} task because: {}", taskId, ex.getMessage());
        }
    }
}
