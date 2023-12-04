package eu.europeana.cloud.service.commons.urls;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class UrlBuilder {

  private Map<UrlPart, String> parts;
  private Set<ValuedUrlPart> partsToInclude = new HashSet<>();

  UrlBuilder(Map<UrlPart, String> parts) {
    partsToInclude = new HashSet<>();
    partsToInclude.add(new ValuedUrlPart(UrlPart.CONTEXT, true));
    this.parts = parts;
  }

  public UrlBuilder clear() {
    partsToInclude = new HashSet<>();
    partsToInclude.add(new ValuedUrlPart(UrlPart.CONTEXT, true));
    return this;
  }

  public UrlBuilder withCloudID() {
    partsToInclude.add(new ValuedUrlPart(UrlPart.RECORDS, true));
    return this;
  }

  public UrlBuilder withRepresentation() {
    partsToInclude.add(new ValuedUrlPart(UrlPart.REPRESENTATIONS, true));
    return this;
  }

  public UrlBuilder withVersion() {
    partsToInclude.add(new ValuedUrlPart(UrlPart.VERSIONS, true));
    return this;
  }

  public UrlBuilder withVersionWithoutValue() {

    partsToInclude.add(new ValuedUrlPart(UrlPart.VERSIONS, false));
    return this;
  }

  public UrlBuilder withDataProvider() {
    partsToInclude.add(new ValuedUrlPart(UrlPart.DATA_PROVIDERS, true));
    return this;
  }

  public UrlBuilder withDataSet() {
    partsToInclude.add(new ValuedUrlPart(UrlPart.DATA_SETS, true));
    return this;
  }

  public UrlBuilder withDataSetWithoutValue() {
    partsToInclude.add(new ValuedUrlPart(UrlPart.DATA_SETS, false));
    return this;
  }


  public String build() throws UrlBuilderException {
    String result = "";

    for (Map.Entry<UrlPart, String> entry : parts.entrySet()) {
      ValuedUrlPart key = new ValuedUrlPart(entry.getKey(), true);
      String value = entry.getValue();
      if (shouldBeIncludedInResult(entry.getKey())) {
        result = buildUrlBasedOnKey(result, entry, key, value);
      }
    }
    if (partsToInclude.size() > 0) {
      String errorMessage = createErrorMessage(partsToInclude);
      throw new UrlBuilderException("Missing parts for given request: " + errorMessage);
    }
    return result;
  }

  private String buildUrlBasedOnKey(String result, Entry<UrlPart, String> entry, ValuedUrlPart key, String value)
      throws UrlBuilderException {
    ValuedUrlPart currentPart = removeFromPartsToInclude(entry.getKey());
    if (currentPart.isAddValue()) {
      if (value != null) {
        if ("".equals(key.getPart().getValue())) {
          result += "/" + value;
        } else {
          result += "/" + key.getPart().getValue() + "/" + value;
        }
      } else {
        throw new UrlBuilderException("Value for: " + key + " is empty");
      }
    } else {
      result += "/" + key.getPart().getValue();
    }
    return result;
  }

  private boolean shouldBeIncludedInResult(UrlPart urlPart) {
    Iterator<ValuedUrlPart> it = partsToInclude.iterator();
    while (it.hasNext()) {
      if (it.next().getPart().equals(urlPart)) {
        return true;
      }
    }
    return false;
  }

  private ValuedUrlPart removeFromPartsToInclude(UrlPart urlPart) throws UrlBuilderException {
    Iterator<ValuedUrlPart> it = partsToInclude.iterator();
    while (it.hasNext()) {
      ValuedUrlPart currentPart = it.next();
      if (currentPart.getPart().equals(urlPart)) {
        it.remove();
        return currentPart;
      }
    }
    throw new UrlBuilderException("The provided parts are not sufficient to build the correct URL");
  }

  private String createErrorMessage(Set<ValuedUrlPart> leftParts) {
    String result = "";
    for (ValuedUrlPart leftPart : leftParts) {
      result += leftPart.getPart() + " ";
    }
    return result;

  }
}

/**
 * Stores url part with information if value of this part should be displayed
 */
class ValuedUrlPart {

  private UrlPart part;
  private boolean addValue;

  ValuedUrlPart(UrlPart part, boolean addValue) {
    this.part = part;
    this.addValue = addValue;
  }

  public UrlPart getPart() {
    return part;
  }

  public boolean isAddValue() {
    return addValue;
  }
}
