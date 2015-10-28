package eu.europeana.cloud.common.model;

/**
 * Available permission values for eCloud resources.
 */
public enum Permission {
    
    ALL(31),
    READ(1),
    WRITE(2),
    DELETE(8),
    ADMINISTRATION(16);
    
    private int intValue;
    private String value;
    
    Permission(int intValue){
        this.intValue = intValue;
    }
    
    public int getIntValue(){
        return this.intValue;
    }
    
    public String getValue(){
        return this.name().toLowerCase();
    }
    
}

