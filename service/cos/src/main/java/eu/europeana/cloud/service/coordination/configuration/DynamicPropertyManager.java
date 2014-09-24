package eu.europeana.cloud.service.coordination.configuration;

/**
 * Can be used to set / retrieve Dynamic Properties.
 * 
 * Dynamic Properties are (String) values that can be automatically updated at Runtime (on-the-fly).
 * 
 * Dynamic properties should be used for those kind of values that are possible to change.
 * If the property is being read only once, using a Dynamic Property is pointless 
 * (the property could just be stored in a configuration file).
 * 
 */
public interface DynamicPropertyManager {

	/**
	 * @param dynamicPropertyUpdatedValue Sets the current value of the specified {{dynamicProperty}} to {{dynamicPropertyUpdatedValue}}
	 */
	void updateValue(final String dynamicProperty, final String dynamicPropertyUpdatedValue);

	/**
	 * @return the current value of the specified {{dynamicProperty}}
	 */
	String getCurrentValue(final String dynamicProperty);

	/**
	 * Subscribes for updates on the specified dynamicProperty.
	 */
	void addUpdateListener(DynamicPropertyListener l, String dynamicProperty);

	/**
	 * Removes the listener.
	 */
	void removeUpdateListener(DynamicPropertyListener l);
}
