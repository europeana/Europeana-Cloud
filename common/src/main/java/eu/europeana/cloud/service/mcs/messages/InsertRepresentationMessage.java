package eu.europeana.cloud.service.mcs.messages;

/**
 * Message about adding a new/updating {@link eu.europeana.cloud.common.model.Representation representation} version of
 * a certain {@link eu.europeana.cloud.common.model.Record record}.
 * 
 */
public class InsertRepresentationMessage extends AbstractMessage {

    /**
     * Constructs InsertRepresentationMessage with given payload
     * 
     * @param payload
     *            message payload
     */
    public InsertRepresentationMessage(String payload) {
        super(payload);
    }
}
