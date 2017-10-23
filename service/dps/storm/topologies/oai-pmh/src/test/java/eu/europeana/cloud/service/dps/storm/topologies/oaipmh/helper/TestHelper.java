package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper;

import org.apache.commons.io.IOUtils;
import org.xmlunit.matchers.CompareMatcher;

import java.io.IOException;
import java.io.InputStream;

import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

/**
 * @author krystian.
 */
public class TestHelper {
    protected static final String DEFAULT_ENCODING = "UTF-8";
    private TestHelper() {
        throw new UnsupportedOperationException("Pure static class!");
    }

    public static String convertToString(InputStream result) throws IOException {
        return new String(IOUtils.toCharArray(result, DEFAULT_ENCODING));
    }

    public static CompareMatcher isSimilarXml(String fileContent) {
        return isSimilarTo(fileContent)
                .ignoreComments()
                .ignoreWhitespace()
                .normalizeWhitespace()
                .throwComparisonFailure();
    }
}
