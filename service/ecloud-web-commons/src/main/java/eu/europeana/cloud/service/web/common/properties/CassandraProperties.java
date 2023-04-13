package eu.europeana.cloud.service.web.common.properties;

import lombok.Getter;
import lombok.Setter;

/**
 * Describes properties related with connection to Cassandra
 */
@Getter
@Setter
public class CassandraProperties {

    private String keyspace;
    private String user;
    private String password;
    private String hosts;
    private int port;

}
