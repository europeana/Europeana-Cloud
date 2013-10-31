package eu.europeana.cloud.common.model;

import java.util.Objects;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents administrative informations about data provider.
 *
 */
@XmlRootElement(name = "properties")
public class DataProviderProperties {

    private String organisationName;

    private String officialAddress;

    private String organisationWebsite;

    private String organisationWebsiteURL;

    private String digitalLibraryWebsite;

    private String digitalLibraryURL;

    private String contactPerson;

    private String remarks;


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


    public DataProviderProperties() {
    }


    public String getOrganisationName() {
        return organisationName;
    }


    public void setOrganisationName(String organisationName) {
        this.organisationName = organisationName;
    }


    public String getOfficialAddress() {
        return officialAddress;
    }


    public void setOfficialAddress(String officialAddress) {
        this.officialAddress = officialAddress;
    }


    public String getOrganisationWebsite() {
        return organisationWebsite;
    }


    public void setOrganisationWebsite(String organisationWebsite) {
        this.organisationWebsite = organisationWebsite;
    }


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


    public String getDigitalLibraryURL() {
        return digitalLibraryURL;
    }


    public void setDigitalLibraryURL(String digitalLibraryURL) {
        this.digitalLibraryURL = digitalLibraryURL;
    }


    public String getContactPerson() {
        return contactPerson;
    }


    public void setContactPerson(String contactPerson) {
        this.contactPerson = contactPerson;
    }


    public String getRemarks() {
        return remarks;
    }


    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }


    @Override
    public int hashCode() {
        int hash = 3;
        hash = 23 * hash + Objects.hashCode(this.organisationName);
        hash = 23 * hash + Objects.hashCode(this.officialAddress);
        hash = 23 * hash + Objects.hashCode(this.organisationWebsite);
        hash = 23 * hash + Objects.hashCode(this.organisationWebsiteURL);
        hash = 23 * hash + Objects.hashCode(this.digitalLibraryWebsite);
        hash = 23 * hash + Objects.hashCode(this.digitalLibraryURL);
        hash = 23 * hash + Objects.hashCode(this.contactPerson);
        hash = 23 * hash + Objects.hashCode(this.remarks);
        return hash;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DataProviderProperties other = (DataProviderProperties) obj;
        if (!Objects.equals(this.organisationName, other.organisationName)) {
            return false;
        }
        if (!Objects.equals(this.officialAddress, other.officialAddress)) {
            return false;
        }
        if (!Objects.equals(this.organisationWebsite, other.organisationWebsite)) {
            return false;
        }
        if (!Objects.equals(this.organisationWebsiteURL, other.organisationWebsiteURL)) {
            return false;
        }
        if (!Objects.equals(this.digitalLibraryWebsite, other.digitalLibraryWebsite)) {
            return false;
        }
        if (!Objects.equals(this.digitalLibraryURL, other.digitalLibraryURL)) {
            return false;
        }
        if (!Objects.equals(this.contactPerson, other.contactPerson)) {
            return false;
        }
        if (!Objects.equals(this.remarks, other.remarks)) {
            return false;
        }
        return true;
    }
}
