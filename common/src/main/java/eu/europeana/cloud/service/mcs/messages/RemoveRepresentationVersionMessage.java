package eu.europeana.cloud.service.mcs.messages;

/**
 * Message about removing a
 * {@link eu.europeana.cloud.common.model.Representation representation} version
 * from a certain {@link eu.europeana.cloud.common.model.Record record}.
 */
public class RemoveRepresentationVersionMessage extends AbstractMessage {

    public RemoveRepresentationVersionMessage(String payload) {
	super(payload);
    }

}
