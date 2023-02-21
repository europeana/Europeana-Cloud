package eu.europeana.cloud.swiftmigrate;

/**
 * Custom {@link SwiftMigrator} copy files from source to target container.
 */
public class CustomFileNameMigrator extends SwiftMigrator {

  /**
   * {@inheritDoc }
   */
  @Override
  public String nameConversion(final String s) {
    return s.contains("|") ? s.replace("|", "_") : null;
  }
}
