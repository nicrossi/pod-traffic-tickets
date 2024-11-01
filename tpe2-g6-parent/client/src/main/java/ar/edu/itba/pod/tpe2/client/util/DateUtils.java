package ar.edu.itba.pod.tpe2.client.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.Validate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DateUtils {
    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    public static LocalDateTime parseDate(String inputDate) {
        Validate.notBlank(inputDate, "Input date cannot be blank");
        return LocalDate.parse(inputDate, INPUT_FORMATTER).atStartOfDay();
    }
}
