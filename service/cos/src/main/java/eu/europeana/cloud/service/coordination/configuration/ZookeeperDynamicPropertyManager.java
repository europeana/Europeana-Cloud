
package eu.europeana.cloud.service.coordination.configuration;

import com.google.common.io.Closeables;
import eu.europeana.cloud.service.coordination.ZookeeperService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Implementation of dynamic properties for ZooKeeper using Curator.
 * 
 * This implementation requires the path to ZK where the dynamic properties are stored.
 * For example: /eCloud/v2/ISTI/configuration/dynamicProperties
 * 
 * Properties are direct ZK child nodes of the root parent ZK node.  
 * An example ZK child property node is /eCloud/v2/ISTI/configuration/dynamicProperties/mcs.swift.address
 * 
 * All the properties are retrieved as {@link String}.
 * 
 * The value is stored in the ZK child property node and can be updated at any time.  
 * All servers will receive a ZK Watcher callback and automatically update their value.
 */
public class ZookeeperDynamicPropertyManager implements DynamicPropertyManager, Closeable {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperDynamicPropertyManager.class);

    private final CuratorFramework client;
    private final String configRootPath;
    private final PathChildrenCache pathChildrenCache;

    private final Charset charset = Charset.forName("UTF-8");
    
    private ConcurrentMap<String, DynamicPropertyListener> listeners = new ConcurrentHashMap<String, DynamicPropertyListener>();

    /**
     * Creates the pathChildrenCache using the CuratorFramework client and ZK root path node for the config
     * 
	 * @param zookeeper service that provides the connection with Zookeeper.
     * @param path to ZK where the dynamic properties are stored (ie. /eCloud/v2/ISTI/configuration/dynamicProperties)
     */
    public ZookeeperDynamicPropertyManager(ZookeeperService zookeeperService, String configRootPath) {
        this.client = zookeeperService.getClient();
        this.configRootPath = configRootPath;        
        this.pathChildrenCache = new PathChildrenCache(client, configRootPath, true);
        
        try {
			start();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
    }    

    /** 
     * Adds a listener to the pathChildrenCache, initializes the cache, then starts the cache-management background thread
     * 
     * @throws Exception
     */
    public void start() throws Exception {
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework aClient, PathChildrenCacheEvent event)
                    throws Exception {
                Type eventType = event.getType();
                ChildData data = event.getData();
                
                String path = null; 
                if (data != null) {
                    path = data.getPath();
                    
                    String key = removeRootPath(path);

                    byte[] value = data.getData();
                    String stringValue = new String(value, charset);

                    LOGGER.debug("received update to pathName [{}], eventType [{}]", path, eventType);
                    LOGGER.debug("key [{}], and value [{}]", key, stringValue);

                    Map<String, Object> added = null;
                    Map<String, Object> changed = null;
                    Map<String, Object> deleted = null;
                    if (eventType == Type.CHILD_ADDED) {
                        added = new HashMap<String, Object>(1);
                        added.put(key, stringValue);
                    } else if (eventType == Type.CHILD_UPDATED) {
                        changed = new HashMap<String, Object>(1);
                        changed.put(key, stringValue);
                    } else if (eventType == Type.CHILD_REMOVED) {
                        deleted = new HashMap<String, Object>(1);
                        deleted.put(key, stringValue);
                    }

                    fireEvent(added);
                    fireEvent(changed);
                    fireEvent(deleted);
                }
            }
        });

        pathChildrenCache.start(StartMode.NORMAL);
    }
    
    public Map<String, Object> getCurrentData() throws Exception {
    	LOGGER.debug("getCurrentData() retrieving current data.");

        List<ChildData> children = pathChildrenCache.getCurrentData();
        Map<String, Object> all = new HashMap<String, Object>(children.size());
        for (ChildData child : children) {
            String path = child.getPath();
            String key = removeRootPath(path);
            byte[] value = child.getData();

            all.put(key, new String(value, charset));
        }

        LOGGER.debug("getCurrentData() retrieved [{}] config elements.", children.size());

        return all;
    }

    @Override
    public void addUpdateListener(DynamicPropertyListener l, String dynamicProperty) {
        if (l != null) {
            listeners.put(dynamicProperty, l);
        }
    }

    @Override
    public void removeUpdateListener(DynamicPropertyListener l) {
        if (l != null) {
            listeners.remove(l);
        }
    }

    protected void fireEvent(Map<String, Object>  updatedProperties) {
    	
    	if (updatedProperties == null) {
    		return;
    	}
    	
    	Iterator<String> dynamicPropertiesIter = listeners.keySet().iterator();
    	while (dynamicPropertiesIter.hasNext()) {
    		String dynamicProperty = dynamicPropertiesIter.next();
    		
    		Object dynamicPropertyValue = updatedProperties.get(dynamicProperty);
    		if (dynamicPropertyValue != null) {
    			listeners.get(dynamicProperty).onUpdate((String) dynamicPropertyValue);
    		}
    	}
    }

    private String removeRootPath(String nodePath) {
        return nodePath.replace(configRootPath + "/", "");
    }  
    
    synchronized void setZkProperty(String key, String value) throws Exception {
        final String path = configRootPath + "/" + key; 

        byte[] data = value.getBytes(charset);
        
        try {
            // attempt to create (intentionally doing this instead of checkExists()) 
            client.create().creatingParentsIfNeeded().forPath(path, data);
        } catch (NodeExistsException exc) {
            // key already exists - update the data instead
            client.setData().forPath(path, data);
        }
    }

    synchronized String getZkProperty(String key) throws Exception {
        final String path = configRootPath + "/" + key; 

        byte[] bytes = client.getData().forPath(path);

        return new String(bytes, charset);
    }
    
    synchronized void deleteZkProperty(String key) throws Exception {
        final String path = configRootPath + "/" + key; 

        try {
            client.delete().forPath(path);        
        } catch (NoNodeException exc) {
        	// Node doesn't exist - NoOp
        	LOGGER.warn("Node doesn't exist", exc);
        }
    }
    
    public void close() {
    	try {
			Closeables.close(pathChildrenCache, true);
		} catch (IOException e) {
			LOGGER.warn(e.getMessage());
		}
    }

	@Override
	public void updateValue(String dynamicProperty,	String dynamicPropertyUpdatedValue) {
		try {
			setZkProperty(dynamicProperty, dynamicPropertyUpdatedValue);
		} catch (Exception e) {
			LOGGER.warn(e.getMessage());
		}
	}

	@Override
	public String getCurrentValue(String dynamicProperty) {
		
		try {
			return getZkProperty(dynamicProperty);
		} catch (Exception e) {
			LOGGER.warn(e.getMessage());
		}
		return null;
	}
}
