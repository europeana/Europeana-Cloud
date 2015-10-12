package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension;


/**
 * Context for choosing the needed extension checker
 */
public class ExtensionCheckerContext {
	private ExtensionChecker extensionChecker;

	/**
	 * Constructs a ExtensionCheckerContext with a specific extensionChecker.
	 *
	 * @param extensionChecker
	 *            the extensionChecker
	 */
	public ExtensionCheckerContext(ExtensionChecker extensionChecker) {
		this.extensionChecker = extensionChecker;
	}

	/**
	 * calling the extension checking method based on the context
	 *
	 * @param filePath the full path of a file
	 * @return boolean value based on the checking process  .
	 */
	public boolean isGoodExtension(String filePath) {
		return extensionChecker.isGoodExtension(filePath);

	}

}
