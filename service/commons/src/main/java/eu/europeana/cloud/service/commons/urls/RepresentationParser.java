package eu.europeana.cloud.service.commons.urls;

import eu.europeana.cloud.common.model.Representation;
import java.net.MalformedURLException;
import java.net.URI;

public final class RepresentationParser {

  private RepresentationParser() {
  }

  public static Representation parseResultUrl(String url) throws MalformedURLException {
    UrlParser parser = new UrlParser(url);
    if (parser.isUrlToRepresentationVersion() || parser.isUrlToRepresentationVersionFile()) {
      Representation rep = new Representation();
      rep.setCloudId(parser.getPart(UrlPart.RECORDS));
      rep.setRepresentationName(parser.getPart(UrlPart.REPRESENTATIONS));
      rep.setVersion(parser.getPart(UrlPart.VERSIONS));
      return rep;
    }
    throw new MalformedURLException("The resulted output URL is not formulated correctly");
  }

  public static Representation parseResultUrl(URI representationUri) throws MalformedURLException {
    return parseResultUrl(representationUri.toString());
  }
}
