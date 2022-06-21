package eu.europeana.cloud.tools.statistics;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ProcessStatisticsApp {
    public static final Logger LOGGER = LoggerFactory.getLogger(ProcessStatisticsApp.class);

    public static void main( String[] args ) {
        mergeFiles(args).ifPresentOrElse(
                ProcessStatisticsApp::processFile,
                ProcessStatisticsApp::showInfo
        );
    }

    public static void showInfo() {
        LOGGER.info("Statistics processor");
        LOGGER.info("At least one parameter with valid existing filename if necessary");
    }

    public static void processFile(Path path) {
        try(Stream<String> lines = Files.lines(path)) {
            lines
                    .map(LogLine::convertLine)
                    .collect(new StatisticsCollector())
                    .forEach((key, value) -> LOGGER.info("Task  {} : {}", key, value));
        } catch(IOException ioException) {
            LOGGER.error("Error while statistics file processing", ioException);
        }
    }

    private static Stream<Path> preparePathsStream(String[] args) {
        return Arrays.stream(args)
                .map(ProcessStatisticsApp::checkFile)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }
    private static List<Path> preparePathsList(String[] args) {
        return preparePathsStream(args).collect(toList());
    }

    private static Optional<Path> checkFile(String s) {
        var path = Path.of(s);
        if(Files.exists(path)) {
            return Optional.of(path);
        }
        return Optional.empty();
    }

    private static List<LogReader> prepareLogReadersList(List<Path> paths) {
        return paths.stream()
                .map(LogReader::new)
                .collect(toList());
    }


    private static Optional<Path> mergeFiles(String[] args) {
        Path result = null;
        var paths = preparePathsList(args);
        if(paths.size() == 1) {
            result = paths.get(0);
        } else if(paths.size() > 1) {
            try {
                result = File.createTempFile("merged_file_", ".log").toPath();
                doMerge(paths, result);
            } catch (IOException ioException) {
                LOGGER.error("Error merging files", ioException);
            }
        }
        return Optional.ofNullable(result);
    }

    private static void doMerge(List<Path> inputPaths, Path outputPath) throws IOException {
        var readers = prepareLogReadersList(inputPaths);

        try (Writer writer = new FileWriter(outputPath.toFile())) {
            var bestLineInfo = new BestLineInfo();
            int index = 0;

            while (!readers.isEmpty()) {
                var currentReaderLine = readers.get(index).peek();

                if (currentReaderLine == null) {
                    readers.remove(index--).close();
                } else {
                    bestLineInfo.checkEarliest(currentReaderLine, index);
                }


                if (index < readers.size() - 1) {
                    index++;
                } else if (!readers.isEmpty()) {
                    var logLine = readers.get(bestLineInfo.getIndex()).pop();
                    if (logLine != null) {
                        writer.write(logLine.toCSV());
                        writer.write('\n');
                    }
                    bestLineInfo.reset();
                    index = 0;
                }
            }
        }
    }

    @Setter
    @Getter
    private static class BestLineInfo {
        private LogLine line;
        private int index;

        public void checkEarliest(LogLine line, int index) {
            if(this.line == null || line.getDateTime().isBefore(this.line.getDateTime())) {
                this.line = line;
                this.index = index;
            }
        }

        public void reset() {
            line = null;
            index = 0;
        }
    }

    public static class LogReader {
        private final Path path;
        private LineNumberReader reader;
        private LogLine currentLine;

        public LogReader(Path path) {
            this.path = path;
        }

        public void close() throws IOException {
            if(reader != null) {
                reader.close();
            }
        }

        public LogLine peek() throws IOException {
            checkReaderOpened();

            if(currentLine == null) {
                var lineAsText = reader.readLine();

                if(lineAsText != null) {
                    currentLine = LogLine.convertLine(lineAsText);
                }
            }
            return currentLine;
        }

        public LogLine pop() throws IOException {
            checkReaderOpened();

            if(currentLine == null) {
                var lineAsText = reader.readLine();

                if(lineAsText != null) {
                    currentLine = LogLine.convertLine(lineAsText);
                }
            }

            var tmpCurrentLine = currentLine;
            currentLine = null;

            return tmpCurrentLine;
        }

        private void checkReaderOpened() throws IOException {
            if(reader == null) {
                reader = new LineNumberReader(new FileReader(path.toFile()));
            }
        }
    }
}
