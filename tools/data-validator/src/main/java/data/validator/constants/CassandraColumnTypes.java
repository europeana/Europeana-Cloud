package data.validator.constants;

/**
 * Created by Tarek on 5/12/2017.
 */
public enum CassandraColumnTypes {
    INTEGER("java.lang.Integer"),
    DATE("java.util.Date"),
    UUID("java.util.UUID"),
    BOOLEAN("java.lang.Boolean"),
    LONG("java.lang.Long");

    private String className;

    CassandraColumnTypes(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }
}


