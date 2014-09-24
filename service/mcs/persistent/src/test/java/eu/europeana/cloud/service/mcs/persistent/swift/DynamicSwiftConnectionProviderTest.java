package eu.europeana.cloud.service.mcs.persistent.swift;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.europeana.cloud.service.coordination.ZookeeperService;
import eu.europeana.cloud.service.coordination.configuration.DynamicPropertyManager;
import eu.europeana.cloud.service.coordination.configuration.ZookeeperDynamicPropertyManager;

/**
 * DynamicSwiftConnectionProviderTest tests.
 * 
 */
public class DynamicSwiftConnectionProviderTest {

	@Test
	public void shouldGetDynamicProperty() {
		
		ZookeeperService zK = new ZookeeperService("ecloud.eanadev.org:2181", 10000, 10000, "/eCloud");
		DynamicPropertyManager dynamicPropertyManager = new ZookeeperDynamicPropertyManager(zK, "/eCloud/v2/ISTI/configuration/dynamicProperties");
		
		DynamicSwiftConnectionProvider dP = new DynamicSwiftConnectionProvider(dynamicPropertyManager);
		
		// lets try to get the current address (its stored in zookeeper as -> "ecloud:user:password:endpoint")
		String address_1 = dP.getSwiftConnectionAddress();
		assertTrue(address_1.equals("ecloud:user:password:endpoint, ecloudX:userX:passwordX:endpointX"));
		
		// lets change it to something else
		String address_2 = "ecloudX:userX:passwordX:endpointX";
		dP.updateSwiftConnectionAddress(address_2);
		
		// test to make sure the address has been updated in zookeeper
		assertTrue(address_2.equals(dP.getSwiftConnectionAddress()));
		
		// change it back to its original value
		dP.updateSwiftConnectionAddress(address_1);
	}
}
