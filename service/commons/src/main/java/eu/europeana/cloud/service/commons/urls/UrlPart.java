package eu.europeana.cloud.service.commons.urls;

public enum UrlPart {
    
    CONTEXT(""),
    DATA_PROVIDERS("data-providers"),
    DATA_SETS("data-sets"),
    RECORDS("records"),
    REPRESENTATIONS("representations"),
    VERSIONS("versions"),
    FILES("files");
    
    private String value;
    
    UrlPart(String value){
        this.value = value;
    }
    
    public String getValue(){
        return value;
    }
}
