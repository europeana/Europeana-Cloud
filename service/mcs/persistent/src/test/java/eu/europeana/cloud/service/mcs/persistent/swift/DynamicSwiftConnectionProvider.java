package eu.europeana.cloud.service.mcs.persistent.swift;


import eu.europeana.cloud.service.coordination.configuration.DynamicPropertyListener;
import eu.europeana.cloud.service.coordination.configuration.DynamicPropertyManager;

/**
 * Example of a {@link DynamicSwiftConnectionProvider}.
 * 
 */
public class DynamicSwiftConnectionProvider implements DynamicPropertyListener {
	
	private final static String SWIFT_DYNAMIC_PROPERTY_NAME = "mcs.swift.addresslist";
	
	private DynamicPropertyManager dynamicPropertyManager;

	public DynamicSwiftConnectionProvider(final DynamicPropertyManager dynamicPropertyManager) {
		
		this.dynamicPropertyManager = dynamicPropertyManager;
		dynamicPropertyManager.addUpdateListener(this, SWIFT_DYNAMIC_PROPERTY_NAME);
	}
	

	/**
	 * Used to get the current property from zookeeper.
	 */
	public String getSwiftConnectionAddress() {
		return dynamicPropertyManager.getCurrentValue(SWIFT_DYNAMIC_PROPERTY_NAME);
	}
	
	/**
	 * Used to update the property in zookeeper.
	 */
	public void updateSwiftConnectionAddress(final String updatedAddress) {
		dynamicPropertyManager.updateValue(SWIFT_DYNAMIC_PROPERTY_NAME, updatedAddress);
	}

	@Override
	/**
	 * Called when the property is updated in Zookeeper (from some other service!)
	 */
	public void onUpdate(String dynamicPropertyUpdate) {
		System.out.println(SWIFT_DYNAMIC_PROPERTY_NAME + "updated:" + dynamicPropertyUpdate);
	}	
}
