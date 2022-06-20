package eu.europeana.cloud.tools.statistics;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Setter
@Getter
@ToString
@Builder
public class LogLine {
    public static final int COLUMNS_IN_LINE = 5;

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private LocalDateTime dateTime;
    private long taskId;

    private boolean operationBegin;
    private String opName;
    private String opId;

    public String getKey() {
        return String.format("%d-%s-%s", taskId, opName, opId);
    }

    public String toCSV() {
        return String.format("%s,%d,%s,%s,%s", dateTime.format(DATE_TIME_FORMATTER), taskId, operationBegin ? "[BEGIN]" : "[END]", opName, opId);
    }

    static LogLine convertLine(String lineAsText) {
        String[] splitLine = lineAsText.split(",");
        if(splitLine.length != COLUMNS_IN_LINE) {
            throw new ProcessStatisticsException(String.format("Bad format of line: '%s'", lineAsText));
        }

        return LogLine.builder()
                .dateTime(LocalDateTime.parse(splitLine[0] , DATE_TIME_FORMATTER))
                .taskId(Long.parseLong(splitLine[1]))
                .operationBegin(splitLine[2].equals("[BEGIN]"))
                .opName(splitLine[3])
                .opId(splitLine[4])
                .build();
    }
}
