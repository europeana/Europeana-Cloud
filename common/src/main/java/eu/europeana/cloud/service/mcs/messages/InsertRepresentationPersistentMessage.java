package eu.europeana.cloud.service.mcs.messages;

/**
 * Message about adding a new persistent {@link eu.europeana.cloud.common.model.Representation representation} version
 * to a certain {@link eu.europeana.cloud.common.model.Record record}.
 */
public class InsertRepresentationPersistentMessage extends AbstractMessage {

    /**
     * Constructs InsertRepresentationPersistentMessage with given payload
     * 
     * @param payload
     *            message payload
     */
    public InsertRepresentationPersistentMessage(String payload) {
        super(payload);
    }

}
