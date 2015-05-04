package eu.europeana.cloud.service.dps.storm.xslt;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.google.common.base.Charsets;

/**
 * Xslt tests
 * 
 * - XSLT fetching from a URL - XSLT compilation - xml transformation
 */
public final class XsltUtilTest {

	// xslt hosted in ISTI (Franco Maria)
	private final String xsltUrl = "http://pomino.isti.cnr.it/~nardini/eCloudTest/a0429_xslt";

	// xslt hosted in ULCC
	private final String ulccXsltUrl = "http://ecloud.eanadev.org:8080/hera/sample_xslt.xslt";

	private final String sampleXmlFileName = "/xmlForTesting.xml";
	private final String sampleXsltFileName = "/sample_xslt.xslt";

	@Test
	public void shouldDownloadXsltFile() throws MalformedURLException,
			IOException {

		String xslt_1 = fetchXslt(xsltUrl);
		String xslt_2 = fetchXslt(ulccXsltUrl);
		assertNotNull(xslt_1);
		assertNotNull(xslt_2);
	}

	@Test
	public void shouldCompileXsltFile() throws MalformedURLException,
			IOException, TransformerException {

		try {
			Source xslDoc = new StreamSource(new URL(ulccXsltUrl).openStream());
			TransformerFactory tFactory = TransformerFactory.newInstance();
			Transformer transformer = tFactory.newTransformer(xslDoc);

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	@Test
	public void shouldPerformXsltTransform() throws MalformedURLException,
			IOException, TransformerException {

		InputStream xmlStream = readXmlInputFile();
		InputStream xsltStream = readXslt();

		Source xslDoc = null;
		Source xmlDoc = null;

		xslDoc = new StreamSource(xsltStream);
		xmlDoc = new StreamSource(xmlStream);

		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer = null;
		StringWriter writer = new StringWriter();

		transformer = tFactory.newTransformer(xslDoc);
		transformer.transform(xmlDoc, new StreamResult(writer));
	}

	private InputStream readXmlInputFile() throws IOException {
		return readFile(sampleXmlFileName);
	}

	private InputStream readXslt() throws IOException {
		return readFile(sampleXsltFileName);
	}

	private InputStream readFile(String fileName) throws IOException {

		assertNotNull(getClass().getResource(fileName));
		String myXml = IOUtils.toString(getClass().getResource(fileName),
				Charsets.UTF_8);

		byte[] bytes = myXml.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(bytes);

		return contentStream;
	}

	private static String fetchXslt(final String xsltUrl) {

		InputStream in = null;
		try {
			in = new URL(xsltUrl).openStream();
			return IOUtils.toString(in);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(in);
		}
		return null;
	}
}
