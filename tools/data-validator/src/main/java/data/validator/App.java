package data.validator;

import com.datastax.driver.core.*;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Iterator;


/**
 * Hello world!
 */
class App {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        // CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider("150.254.164.7,150.254.164.8,150.254.164.9,150.254.164.10,150.254.164.11,150.254.164.12", 9042, "production_ecloud_mcs", "production_ecloud_mcs_user", "9aYxzUc7UznEhh3rzrT8");
        // PreparedStatement selectStatement = cassandraConnectionProvider.getSession().prepare("select * from data_sets");

        ApplicationContext context =
                new ClassPathXmlApplicationContext(new String[]{"data-validator-context.xml"});

        DataValidator dataValidator = (DataValidator) context.getBean("dataValidator");


        dataValidator.validate("cassandra_migration_version", "cassandra_migration_version",10);

        /*
        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        BoundStatement boundStatement = selectStatement.bind();
       // boundStatement.setFetchSize(1000);
        long count = 0;
        ResultSet rs = cassandraConnectionProvider.getSession().execute(boundStatement);
        Iterator<Row> iterator = rs.iterator();
        while (iterator.hasNext()) {
            count++;
            System.out.println(count);
            iterator.next();
        }

        System.out.println(count);
        System.out.println("*******************************************************");


        String tar = "SELECT column_name FROM system.schema_columns" +
                "  WHERE keyspace_name = 'production_ecloud_mcs' AND columnfamily_name = 'data_sets';";
        selectStatement = cassandraConnectionProvider.getSession().prepare(tar);
        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        boundStatement = selectStatement.bind();
        boundStatement.setFetchSize(1000);
        rs = cassandraConnectionProvider.getSession().execute(boundStatement);
        iterator = rs.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().getString(0));
        }

        // rs = cassandraConnectionProvider.getSession().execute(stmt);
        */
    }
}

