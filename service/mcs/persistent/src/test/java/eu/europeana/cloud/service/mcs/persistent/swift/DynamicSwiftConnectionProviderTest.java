package eu.europeana.cloud.service.mcs.persistent.swift;

import eu.europeana.cloud.service.coordination.ZookeeperService;
import eu.europeana.cloud.service.coordination.configuration.DynamicPropertyManager;
import eu.europeana.cloud.service.coordination.configuration.ZookeeperDynamicPropertyManager;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * DynamicSwiftConnectionProviderTest tests.
 * 
 */
public class DynamicSwiftConnectionProviderTest {

    // @Test
    public void shouldGetDynamicProperty() {

	ZookeeperService zK = new ZookeeperService("192.168.12.42:2181", 10000,
		10000, "/zk");
	DynamicPropertyManager dynamicPropertyManager = new ZookeeperDynamicPropertyManager(
		zK, "/eCloud/v2/ISTI/configuration/dynamicProperties");
	fail();
	if (false) {
	    ZookeeperSwiftConnectionProvider dP = new ZookeeperSwiftConnectionProvider(
		    dynamicPropertyManager);

	    // lets try to get the current address (its stored in zookeeper as
	    // ->
	    // "ecloud:user:password:endpoint")
	    String address_1 = dP.getSwiftConnectionAddress();
	    assertTrue(address_1
		    .equals("ecloud:user:password:endpoint, ecloudX:userX:passwordX:endpointX"));

	    // lets change it to something else
	    String address_2 = "ecloudX:userX:passwordX:endpointX";
	    dP.updateSwiftConnectionAddress(address_2);

	    // test to make sure the address has been updated in zookeeper
	    assertTrue(address_2.equals(dP.getSwiftConnectionAddress()));

	    // change it back to its original value
	    dP.updateSwiftConnectionAddress(address_1);
	}
    }
}
