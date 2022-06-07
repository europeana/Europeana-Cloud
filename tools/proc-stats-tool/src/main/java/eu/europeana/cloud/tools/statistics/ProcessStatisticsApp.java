package eu.europeana.cloud.tools.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
public class ProcessStatisticsApp {
    public static final Logger LOGGER = LoggerFactory.getLogger(ProcessStatisticsApp.class);
    public static final int COLUMNS_IN_LINE = 5;

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main( String[] args ) {
        checkFile(args).ifPresentOrElse(
                ProcessStatisticsApp::processFile,
                ProcessStatisticsApp::showInfo
        );
    }

    public static Optional<Path> checkFile(String[] args) {
        if(args.length > 0) {
            var path = Path.of(args[0]);
            if(Files.exists(path)) {
                return Optional.of(path);
            }
        }
        return Optional.empty();
    }

    public static void showInfo() {
        LOGGER.info("Statistics processor");
        LOGGER.info("At least one parameter with valid existing filename if necessary. Second and next parameters if occurs will be ignored");
    }

    public static void processFile(Path path) {
        try(Stream<String> lines = Files.lines(path)) {
            lines
                    .map(ProcessStatisticsApp::convertLine)
                    .collect(new StatisticsCollector())
                    .forEach((key, value) -> LOGGER.info("Task  {} : {}", key, value));
        } catch(IOException ioException) {
            LOGGER.error("Error while statistics file processing", ioException);
        }
    }

    private static Line convertLine(String lineAsText) {
        String[] splitLine = lineAsText.split(",");
        if(splitLine.length != COLUMNS_IN_LINE) {
            throw new ProcessStatisticsException(String.format("Bad format of line: '%s'", lineAsText));
        }

        return Line.builder()
                .dateTime(LocalDateTime.parse(splitLine[0] , DATE_TIME_FORMATTER))
                .taskId(Long.parseLong(splitLine[1]))
                .startLine(splitLine[2].equals("[BEGIN]"))
                .opName(splitLine[3])
                .opId(splitLine[4])
                .build();
    }
}
