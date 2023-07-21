package eu.europeana.cloud.common.utils;

/**
 * Created by Tarek on 8/3/2016.
 */
public enum Tags {
  DELETED("deleted");
  private String value;

  Tags(String value) {
    this.value = value;
  }

  public String getTag() {
    return this.value;
  }

}
