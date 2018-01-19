package migrations.service.dps.V3;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V3_1__create_statistics_tables implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute("CREATE TABLE general_statistics (\n" +
                "    task_id bigint,\n" +
                "    parent_xpath varchar,\n" +
                "    node_xpath varchar,\n" +
                "    occurrence counter,\n" +
                "    PRIMARY KEY(task_id, parent_xpath, node_xpath)\n" +
                ");\n");
        session.execute("CREATE TABLE node_statistics (\n" +
                "    task_id bigint,\n" +
                "    node_xpath varchar,\n" +
                "    node_value varchar,\n" +
                "    occurence counter,\n" +
                "    PRIMARY KEY((task_id, node_xpath), node_value)\n" +
                ");\n");
        session.execute("CREATE TABLE attribute_statistics (\n" +
                "    task_id bigint,\n" +
                "    node_xpath varchar,\n" +
                "    node_value varchar,\n" +
                "    attribute_name varchar,\n" +
                "    attribute_value varchar,\n" +
                "    occurrence counter,\n" +
                "    PRIMARY KEY((task_id, node_xpath, node_value), attribute_name, attribute_value)\n" +
                ");\n");
    }
}
