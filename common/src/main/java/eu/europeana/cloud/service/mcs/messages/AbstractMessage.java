package eu.europeana.cloud.service.mcs.messages;

import java.io.Serializable;
import java.util.Objects;

/**
 * Message for MCS and DLS asynchronous communication.
 * 
 */
public abstract class AbstractMessage implements Serializable {

    String payload;

    public AbstractMessage(String payload) {
	this.payload = payload;
    }

    public String getPayload() {
	return payload;
    }

    @Override
    public int hashCode() {
	return payload.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
	if (obj == null) {
	    return false;
	}
	if (getClass() != obj.getClass()) {
	    return false;
	}
	final AbstractMessage other = (AbstractMessage) obj;
	if (!Objects.equals(this.payload, other.payload)) {
	    return false;
	}
	return true;
    }
}
