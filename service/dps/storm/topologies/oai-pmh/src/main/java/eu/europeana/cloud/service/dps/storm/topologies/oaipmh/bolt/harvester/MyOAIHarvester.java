package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import com.google.common.io.ByteSource;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.dspace.xoai.serviceprovider.client.OAIClient;
import org.dspace.xoai.serviceprovider.exceptions.HttpException;
import org.dspace.xoai.serviceprovider.parameters.Parameters;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.zip.GZIPInputStream;


public class MyOAIHarvester implements OAIClient {
        private String baseUrl;
        private HttpClient httpclient;
        private int timeout = 60000;
        private String userAgent;
        private List<String> baseUrlsHttpsExclusion;

        public MyOAIHarvester(String baseUrl) {
            this.baseUrl = baseUrl;
            httpclient = new DefaultHttpClient(createHttpParams());
//            httpclient = HttpClientBuilder.create().build();
        }


        /**
         * Creates a HttpOAIClient
         *
         * @param baseUrl - the base URL for the OAI repository
         * @param baseUrlsHttpsExclusion - if provided, the base URLs for the OAI repositories will ignore problems related with HTTPS certificate verification
         * @throws HttpException
         */
        public MyOAIHarvester(String baseUrl, List<String> baseUrlsHttpsExclusion) throws HttpException {
            this.baseUrl = baseUrl;
            this.baseUrlsHttpsExclusion = baseUrlsHttpsExclusion;
            initHttpClient();
        }


        /**
         * Creates a HttpOAIClient
         *
         * @param baseUrl - the base URL for the OAI repository
         * @param baseUrlsHttpsExclusion - if provided, the base URLs for the OAI repositories will ignore problems related with HTTPS certificate verification
         * @param timeout - timeout for HTTP connections
         * @throws HttpException
         */
        public MyOAIHarvester(String baseUrl, List<String> baseUrlsHttpsExclusion, int timeout) throws HttpException {
            this.timeout = timeout;
            this.baseUrl = baseUrl;
            this.baseUrlsHttpsExclusion = baseUrlsHttpsExclusion;
            initHttpClient();
        }


        /**
         * Creates a HttpOAIClient
         *
         * @param baseUrl - the base URL for the OAI repository
         * @param baseUrlsHttpsExclusion - if provided, the base URLs for the OAI repositories will ignore problems related with HTTPS certificate verification
         * @param timeout - timeout for HTTP connections
         * @param userAgent - the user agent to be used when communicating with the repository
         * @throws HttpException
         */
        public MyOAIHarvester(String baseUrl, List<String> baseUrlsHttpsExclusion, int timeout, String userAgent) throws HttpException {
            this.timeout = timeout;
            this.baseUrl = baseUrl;
            this.baseUrlsHttpsExclusion = baseUrlsHttpsExclusion;
            this.userAgent = userAgent;
            initHttpClient();
        }

        @Override
        public InputStream execute(Parameters parameters) throws HttpException {
            try {
                HttpResponse response = httpclient
                        .execute(createGetRequest(parameters));
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    Header encodingHeader = response.getFirstHeader("Content-Encoding");
                    if(encodingHeader!=null)
                        if(encodingHeader.getValue().equals("gzip")) {
                            return decompressInputStream(new GZIPInputStream(response.getEntity().getContent()));
                        }
                    return response.getEntity().getContent();
                }
                else
                    throw new HttpException(
                            "Error querying service. Returned HTTP Status Code: "
                                    + response.getStatusLine().getStatusCode());
            } catch (IOException e) {
                throw new HttpException(e);
            }
        }

        public InputStream decompressInputStream(GZIPInputStream gzipInputStream) throws IOException {
            return ByteSource.wrap(IOUtils.toByteArray(gzipInputStream)).openStream();
        }

        private HttpUriRequest createGetRequest(Parameters parameters) {
            HttpGet httpGet = new HttpGet(parameters.toUrl(baseUrl));
            httpGet.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");
            return httpGet;
        }

        /**
         * Initializes the HTTP client and if the base URL is in the
         * baseUrlsHttpsExclusion then the certificate verification will not happen
         *
         * @throws HttpException
         */
        private void initHttpClient() throws HttpException {
            try {
                if (baseUrlsHttpsExclusion != null && baseUrlsHttpsExclusion.contains(baseUrl)) {
                    SSLSocketFactory sslsf = new SSLSocketFactory(
                            new TrustStrategy() {
                                @Override
                                public boolean isTrusted(X509Certificate[] chain,
                                                         String authType)
                                        throws CertificateException {
                                    return true;
                                }

                            }, new AllowAllHostnameVerifier());

                    Scheme httpsScheme = new Scheme("https", 443, sslsf);
                    SchemeRegistry schemeRegistry = new SchemeRegistry();
                    schemeRegistry.register(httpsScheme);

                    ClientConnectionManager cm = new BasicClientConnectionManager(
                            schemeRegistry);

                    httpclient = new DefaultHttpClient(cm, createHttpParams());
                } else {
                    httpclient = new DefaultHttpClient(createHttpParams());

                }
            } catch (KeyManagementException e) {
                throw new HttpException(e);
            } catch (UnrecoverableKeyException e) {
                throw new HttpException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new HttpException(e);
            } catch (KeyStoreException e) {
                throw new HttpException(e);
            }
        }

        /**
         * Creates a HttpParams with the options connection and socket timeout (default timeout if none is defined: 1
         * minute)
         *
         * @return
         */
        private HttpParams createHttpParams() {
            final HttpParams httpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(httpParams, timeout);
            HttpConnectionParams.setSoTimeout(httpParams, timeout);
            // We only set the user agent if it was initialized. Otherwise, since we will not set this parameter,
            // the apache default will be used.
            if(userAgent != null) {
                httpParams.setParameter(CoreProtocolPNames.USER_AGENT, userAgent);
            }
            return httpParams;
        }
    }

