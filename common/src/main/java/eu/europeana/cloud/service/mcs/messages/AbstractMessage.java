package eu.europeana.cloud.service.mcs.messages;

/**
 * Message for MCS and DLS asynchronous communication.
 * 
 */
public abstract class AbstractMessage {

    String payload;

    public AbstractMessage(String payload) {
	this.payload = payload;

    }
}
