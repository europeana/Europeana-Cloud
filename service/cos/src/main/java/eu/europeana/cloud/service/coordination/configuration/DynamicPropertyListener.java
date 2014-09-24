package eu.europeana.cloud.service.coordination.configuration;



/**
 * The listener is called when a dynamic property is updated.
 */
public interface DynamicPropertyListener {
	
    /**
     * Called when a dynamic property is updated.
     */
    public void onUpdate(String dynamicPropertyUpdate);
}
