package eu.europeana.cloud.data;

/**
 * Created by Tarek on 7/15/2019.
 */
public class RevisionInformation {

  private String dataSet;
  private String providerId;
  private String representationName;
  private String revisionName;
  private String revisionProvider;
  private String revisionTimeStamp;


  public RevisionInformation(String dataSet, String providerId, String representationName, String revisionName,
      String revisionProvider, String revisionTimeStamp) {
    this.dataSet = dataSet;
    this.providerId = providerId;
    this.representationName = representationName;
    this.revisionName = revisionName;
    this.revisionProvider = revisionProvider;
    this.revisionTimeStamp = revisionTimeStamp;
  }

  public String getDataSet() {
    return dataSet;
  }

  public void setDataSet(String dataSet) {
    this.dataSet = dataSet;
  }

  public String getProviderId() {
    return providerId;
  }

  public void setProviderId(String providerId) {
    this.providerId = providerId;
  }

  public String getRepresentationName() {
    return representationName;
  }

  public void setRepresentationName(String representationName) {
    this.representationName = representationName;
  }

  public String getRevisionName() {
    return revisionName;
  }

  public void setRevisionName(String revisionName) {
    this.revisionName = revisionName;
  }

  public String getRevisionProvider() {
    return revisionProvider;
  }

  public void setRevisionProvider(String revisionProvider) {
    this.revisionProvider = revisionProvider;
  }

  public String getRevisionTimeStamp() {
    return revisionTimeStamp;
  }

  public void setRevisionTimeStamp(String revisionTimeStamp) {
    this.revisionTimeStamp = revisionTimeStamp;
  }
}
