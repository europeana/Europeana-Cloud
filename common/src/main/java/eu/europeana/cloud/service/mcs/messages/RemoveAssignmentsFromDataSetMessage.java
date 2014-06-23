package eu.europeana.cloud.service.mcs.messages;

/**
 * Message about removing all assignments from a certain
 * {@link eu.europeana.cloud.common.model.DataSet data set}.
 */
public class RemoveAssignmentsFromDataSetMessage extends AbstractMessage {

    public RemoveAssignmentsFromDataSetMessage(String payload) {
	super(payload);
    }

}
