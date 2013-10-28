package eu.europeana.cloud.test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.ws.rs.core.Configuration;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnector;
import org.glassfish.jersey.internal.util.PropertiesHelper;

/**
 * Original HttpUrlConnector does not implement chunked output (why???) and other implementations (Grizzly, Apache) do not work
 * because of other issues (they do not set boundary for multipart). This is an ugly workaround but works - it allows
 * to send huge files by chunking request streams.
 */
public class ChunkedHttpUrlConnector extends HttpUrlConnector {

    public ChunkedHttpUrlConnector(final Configuration config) {
        super(new HttpUrlConnector.ConnectionFactory() {

            @Override
            public HttpURLConnection getConnection(URL url)
                    throws IOException {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setDoInput(true);
                con.setDoOutput(true);
                con.setChunkedStreamingMode(PropertiesHelper.getValue(config.getProperties(),
                        ClientProperties.CHUNKED_ENCODING_SIZE, -1));
                return con;
            }
        });
    }
}