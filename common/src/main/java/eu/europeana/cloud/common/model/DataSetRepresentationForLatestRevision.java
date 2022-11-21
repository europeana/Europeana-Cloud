package eu.europeana.cloud.common.model;

/**
 * Created by pwozniak on 11/21/16.
 */
public class DataSetRepresentationForLatestRevision {

  private Representation representation;
  private Revision revision;

  public Representation getRepresentation() {
    return representation;
  }

  public void setRepresentation(Representation representation) {
    this.representation = representation;
  }

  public Revision getRevision() {
    return revision;
  }

  public void setRevision(Revision revision) {
    this.revision = revision;
  }

}
