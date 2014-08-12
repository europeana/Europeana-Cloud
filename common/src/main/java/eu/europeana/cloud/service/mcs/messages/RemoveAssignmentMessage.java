package eu.europeana.cloud.service.mcs.messages;

/**
 * Message about removing an assignment of {@link eu.europeana.cloud.common.model.Representation representation} of
 * certain representation name to a certain {@link eu.europeana.cloud.common.model.DataSet data set}.
 */
public class RemoveAssignmentMessage extends AbstractMessage {

    /**
     * Constructs RemoveAssignmentMessage with given payload
     * 
     * @param payload
     *            message payload
     */
    public RemoveAssignmentMessage(String payload) {
        super(payload);
    }

}
