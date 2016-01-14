package eu.europeana.cloud.migrator;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.StringTokenizer;

/**
 * Created by helin on 2015-12-23.
 */
public class Cleaner {


    public void clean(String providerId, RecordServiceClient mcs, UISClient uis) {
        try {
            for (String line : Files.readAllLines(FileSystems.getDefault().getPath(".", providerId + ".txt"), Charset.forName("UTF-8"))) {
                StringTokenizer st = new StringTokenizer(line, "=");
                if (st.hasMoreTokens()) {
                    st.nextToken();
                }
                String url = st.nextToken();
                int pos = url.indexOf("/records/");
                if (pos > -1) {
                    String id = url.substring(pos + "/records/".length());
                    id = id.substring(0, id.indexOf("/"));
                    mcs.deleteRecord(id);
                    uis.deleteCloudId(id);
                }
            }
        } catch (IOException e) {
        } catch (RecordNotExistsException e) {
            e.printStackTrace();
        } catch (MCSException e) {
            e.printStackTrace();
        } catch (CloudException e) {
            e.printStackTrace();
        }

    }
}
