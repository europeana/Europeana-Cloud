package eu.europeana.cloud.service.ics.converter.converter;


import eu.europeana.cloud.service.ics.converter.command.KakaduCompressCommandBuilder;
import eu.europeana.cloud.service.ics.converter.extension.ExtensionCheckerContext;
import eu.europeana.cloud.service.ics.converter.extension.JP2ExtensionChecker;
import eu.europeana.cloud.service.ics.converter.extension.TiffExtensionChecker;
import eu.europeana.cloud.service.ics.converter.command.CommandBuilderContext;

/**
 * Service for converting Tiff file to Jp2 file by executing kakadu compress shell command
 */
public class KakaduConverterTiffToJP2 extends ConsoleBasedConverter {
	public KakaduConverterTiffToJP2() {
		super(new CommandBuilderContext(new KakaduCompressCommandBuilder()),new ExtensionCheckerContext(new TiffExtensionChecker()),new ExtensionCheckerContext(new JP2ExtensionChecker()));

	}
}
