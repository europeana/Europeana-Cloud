package eu.europeana.cloud.service.mcs.messages;

/**
 * Message about adding an assignment of {@link eu.europeana.cloud.common.model.Representation representation} version a
 * certain {@link eu.europeana.cloud.common.model.DataSet data set}.
 */
public class AddAssignmentMessage extends AbstractMessage {

    /**
     * Constructs AddAssignmentMessage with given payload
     * 
     * @param payload
     *            message payload
     */
    public AddAssignmentMessage(String payload) {
        super(payload);
    }

}
