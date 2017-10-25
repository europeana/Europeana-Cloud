package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter;


import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command.CommandBuilderContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command.KakaduCompressCommandBuilder;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension.ExtensionCheckerContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension.JP2ExtensionChecker;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension.TiffExtensionChecker;

/**
 * Service for converting Tiff file to Jp2 file by executing kakadu compress shell command
 */
public class KakaduConverterTiffToJP2 extends ConsoleBasedConverter {
	public KakaduConverterTiffToJP2() {
		super(new CommandBuilderContext(new KakaduCompressCommandBuilder()),new ExtensionCheckerContext(new TiffExtensionChecker()),new ExtensionCheckerContext(new JP2ExtensionChecker()));

	}
}
