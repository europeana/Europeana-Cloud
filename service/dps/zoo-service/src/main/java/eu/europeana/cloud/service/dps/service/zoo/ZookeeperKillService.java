package eu.europeana.cloud.service.dps.service.zoo;

import eu.europeana.cloud.service.coordination.ZookeeperService;
import eu.europeana.cloud.service.dps.TaskExecutionKillService;
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
        throw  new UnsupportedOperationException("This operation is not supported yet");

    }    

    @Override
    public Boolean hasKillFlag(String topology, long taskId) 
    {
        throw  new UnsupportedOperationException("This operation is not supported yet");
    }

    @Override
    public void cleanOldFlags(String topology, long ttl) 
    {
        throw  new UnsupportedOperationException("This operation is not supported yet");
    }

    @Override
    public void removeFlag(String topology, long taskId) 
    {
        throw  new UnsupportedOperationException("This operation is not supported yet");
    }
}
