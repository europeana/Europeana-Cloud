package eu.europeana.cloud.common.utils;

/**
 * Created by Tarek on 8/3/2016.
 */
public enum Tags {
    ACCEPTANCE("acceptance"), PUBLISHED("published"), DELETED("deleted");
    private String name;
    private boolean value;

    Tags(String name) {
        this.name = name;
    }

    public String getTag() {
        return this.name;
    }

    public void setValue(boolean value){
        this.value = value;
    }
    public boolean getValue(){
        return this.value;
    }
}
