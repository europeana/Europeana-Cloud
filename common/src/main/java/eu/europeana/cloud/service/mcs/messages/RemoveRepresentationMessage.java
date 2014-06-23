package eu.europeana.cloud.service.mcs.messages;

/**
 * Message about removing a
 * {@link eu.europeana.cloud.common.model.Representation representation} of
 * certain representation name in all versions from a
 * {@link eu.europeana.cloud.common.model.Record record} of certain cloudId.
 */
public class RemoveRepresentationMessage extends AbstractMessage {

    public RemoveRepresentationMessage(String payload) {
	super(payload);
    }

}
