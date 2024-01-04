package eu.europeana.cloud.common.model;

import lombok.EqualsAndHashCode;

import java.util.Objects;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents administrative informations about data provider.
 */
@XmlRootElement(name = "properties")
@EqualsAndHashCode
public class DataProviderProperties {

  /* Organisation name */
  private String organisationName;

  /* The official Address */
  private String officialAddress;

  /* Organisation Website */
  private String organisationWebsite;

  /* Organisation website URL */
  private String organisationWebsiteURL;

  /* Digital Library website */
  private String digitalLibraryWebsite;

  /* Digital Library website URL */
  private String digitalLibraryURL;

  /* Contact person */
  private String contactPerson;

  /* Remarks */
  private String remarks;

  /**
   * Creates a new instance of this class.
   *
   * @param organisationName organization name
   * @param officialAddress organization official address
   * @param organisationWebsite organization website
   * @param organisationWebsiteURL organization website address
   * @param digitalLibraryWebsite digital library website
   * @param digitalLibraryURL digital library website URL
   * @param contactPerson contact person
   * @param remarks remarks
   */
  public DataProviderProperties(String organisationName, String officialAddress, String organisationWebsite,
      String organisationWebsiteURL, String digitalLibraryWebsite, String digitalLibraryURL,
      String contactPerson, String remarks) {
    super();
    this.organisationName = organisationName;
    this.officialAddress = officialAddress;
    this.organisationWebsite = organisationWebsite;
    this.organisationWebsiteURL = organisationWebsiteURL;
    this.digitalLibraryWebsite = digitalLibraryWebsite;
    this.digitalLibraryURL = digitalLibraryURL;
    this.contactPerson = contactPerson;
    this.remarks = remarks;
  }

  /**
   * Creates a new instance of this class.
   */
  public DataProviderProperties() {
  }

  /**
   * @return Organisation name
   */
  public String getOrganisationName() {
    return organisationName;
  }

  public void setOrganisationName(String organisationName) {
    this.organisationName = organisationName;
  }

  /**
   * @return The official Address
   */
  public String getOfficialAddress() {
    return officialAddress;
  }

  public void setOfficialAddress(String officialAddress) {
    this.officialAddress = officialAddress;
  }

  /**
   * @return Organisation Website
   */
  public String getOrganisationWebsite() {
    return organisationWebsite;
  }

  public void setOrganisationWebsite(String organisationWebsite) {
    this.organisationWebsite = organisationWebsite;
  }

  /**
   * @return Organisation website URL
   */
  public String getOrganisationWebsiteURL() {
    return organisationWebsiteURL;
  }

  public void setOrganisationWebsiteURL(String organisationWebsiteURL) {
    this.organisationWebsiteURL = organisationWebsiteURL;
  }

  public String getDigitalLibraryWebsite() {
    return digitalLibraryWebsite;
  }

  public void setDigitalLibraryWebsite(String digitalLibraryWebsite) {
    this.digitalLibraryWebsite = digitalLibraryWebsite;
  }

  /**
   * @return Organisation website URL
   */
  public String getDigitalLibraryURL() {
    return digitalLibraryURL;
  }

  public void setDigitalLibraryURL(String digitalLibraryURL) {
    this.digitalLibraryURL = digitalLibraryURL;
  }

  /**
   * @return Contact person
   */
  public String getContactPerson() {
    return contactPerson;
  }

  public void setContactPerson(String contactPerson) {
    this.contactPerson = contactPerson;
  }

  /**
   * @return Remarks
   */
  public String getRemarks() {
    return remarks;
  }

  public void setRemarks(String remarks) {
    this.remarks = remarks;
  }

  @Override
  public String toString() {

    StringBuffer sb = new StringBuffer();
    sb.append("\n--Organisation properties--");
    sb.append("\nOrganisation=");
    sb.append(organisationName);
    sb.append("\nOfficialAddress=");
    sb.append(officialAddress);
    sb.append("\nOrganisationWebsite=");
    sb.append(organisationWebsite);
    sb.append("\nWebsite=");
    sb.append(organisationWebsiteURL);
    sb.append("\ndigitalLibraryWebsite=");
    sb.append(digitalLibraryWebsite);
    sb.append("\ndigitalLibraryURL=");
    sb.append(digitalLibraryURL);
    sb.append("\ncontactPerson=");
    sb.append(contactPerson);
    sb.append("\nremarks=");
    sb.append(remarks);
    sb.append("\n--");
    return sb.toString();
  }

}
