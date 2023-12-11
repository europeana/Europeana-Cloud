package eu.europeana.cloud.common.properties;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

/**
 * Describes properties related with connection to Cassandra
 */
@Getter
@Setter
public class CassandraProperties implements Serializable {

    private String keyspace;
    private String user;
    private String password;
    private String hosts;
    private int port;

}
