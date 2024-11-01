package ar.edu.itba.pod.tpe2.client.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.Validate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DateUtils {
    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter NYC_INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter CHI_INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static LocalDateTime parseDate(String inputDate) {
        Validate.notBlank(inputDate, "Input date cannot be blank");
        return LocalDate.parse(inputDate, INPUT_FORMATTER).atStartOfDay();
    }

    public static LocalDateTime parseDateNYC(String inputDate) {
        Validate.notBlank(inputDate, "Input date cannot be blank");
        return LocalDate.parse(inputDate, NYC_INPUT_FORMATTER).atStartOfDay();
    }

    public static LocalDateTime parseDateCHI(String inputDate) {
        Validate.notBlank(inputDate, "Input date cannot be blank");
        return LocalDateTime.parse(inputDate, CHI_INPUT_FORMATTER);
    }
}
