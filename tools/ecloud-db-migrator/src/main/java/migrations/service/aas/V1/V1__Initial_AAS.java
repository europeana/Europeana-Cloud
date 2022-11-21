package migrations.service.aas.V1;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V1__Initial_AAS implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute(
        "CREATE TABLE aois (\n" +
            "    id text PRIMARY KEY,\n" +
            "    isinheriting boolean,\n" +
            "    isownerprincipal boolean,\n" +
            "    objclass text,\n" +
            "    objid text,\n" +
            "    owner text,\n" +
            "    parentobjclass text,\n" +
            "    parentobjid text\n" +
            ");\n");

    session.execute(
        "CREATE TABLE acls (\n" +
            "    id text,\n" +
            "    sid text,\n" +
            "    aclorder int,\n" +
            "    isauditfailure boolean,\n" +
            "    isauditsuccess boolean,\n" +
            "    isgranting boolean,\n" +
            "    issidprincipal boolean,\n" +
            "    mask int,\n" +
            "    PRIMARY KEY (id, sid, aclorder)\n" +
            ") WITH CLUSTERING ORDER BY (sid ASC, aclorder ASC);\n");

    session.execute(
        "CREATE TABLE children (\n" +
            "    id text,\n" +
            "    childid text,\n" +
            "    objclass text,\n" +
            "    objid text,\n" +
            "    PRIMARY KEY (id, childid)\n" +
            ") WITH CLUSTERING ORDER BY (childid ASC);\n");

    session.execute(
        "CREATE TABLE users (\n" +
            "    username text PRIMARY KEY,\n" +
            "    password text,\n" +
            "    roles set<text>\n" +
            ");\n");

  }
}
