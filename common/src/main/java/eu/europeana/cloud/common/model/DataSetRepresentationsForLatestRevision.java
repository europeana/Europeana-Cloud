package eu.europeana.cloud.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pwozniak on 11/21/16.
 */
public class DataSetRepresentationsForLatestRevision {

	private DataSet dataset;
	private List<Representation> representations = new ArrayList<Representation>();
	private Revision revision;

	public List<Representation> getRepresentations() {
		return representations;
	}

	public void addRepresentation(Representation representation) {
		representations.add(representation);
	}

	public Revision getRevision() {
		return revision;
	}

	public void setRevision(Revision revision) {
		this.revision = revision;
	}

	public DataSet getDataset() {
		return dataset;
	}

	public void setDataset(DataSet dataset) {
		this.dataset = dataset;
	}
}
