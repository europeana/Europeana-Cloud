package eu.europeana.cloud.service.commons.urls;

import eu.europeana.cloud.common.model.DataSet;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DataSetUrlParser {

  private DataSetUrlParser() {
  }

  public static DataSet parse(String url) throws MalformedURLException {
    UrlParser parser = new UrlParser(url);
    if (parser.isUrlToDataset()) {
      DataSet dataSet = new DataSet();
      dataSet.setId(parser.getPart(UrlPart.DATA_SETS));
      dataSet.setProviderId(parser.getPart(UrlPart.DATA_PROVIDERS));
      return dataSet;
    }
    throw new MalformedURLException("The dataSet URL is not formulated correctly");

  }

  public static List<DataSet> parseList(String urlList) throws MalformedURLException {
    if (urlList == null) {
      return Collections.emptyList();
    }

    List<DataSet> result = new ArrayList<>();
    for (String url : urlList.split(",")) {
      result.add(parse(url));
    }
    return result;
  }

}
