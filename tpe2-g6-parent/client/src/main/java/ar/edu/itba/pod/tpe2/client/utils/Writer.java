package ar.edu.itba.pod.tpe2.client.utils;

import org.apache.commons.lang3.Validate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Writer {
    private List<String> logs = new ArrayList<>();
    private List<String> results = new ArrayList<>();
    private String outPath, query;
    
    public Writer() {
        outPath = Validate.notBlank(System.getProperty("outPath"));
        query = Validate.notBlank(System.getProperty("query"));
    }

    public String dateFormatted(Date date) {
        Instant instant = date.toInstant();
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss:SSSS");
        return fmt.format(instant.atZone(ZoneId.systemDefault()));
    }

    public void addLog(String logPattern, Date date) {
        String log = logPattern.formatted(dateFormatted(date));
        System.out.println(log);
        logs.add(log);
    }
    public void outputLogs() {
        String path = outPath + "time" + query + ".txt";

        StringBuilder builder = new StringBuilder();
        logs.stream().forEach(log -> builder.append(log).append("\n"));

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(path, false))) {
            writer.write(builder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addResult(String result) {
        results.add(result);
    }

    public void outputResults(String[] headers) {
        String path = outPath + "query" + query + ".csv";

        StringBuilder builder = new StringBuilder();
        builder.append(String.join(";", headers)).append("\n");
        results.stream().forEach(res -> builder.append(res).append("\n"));

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(path, false))) {
            writer.write(builder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
