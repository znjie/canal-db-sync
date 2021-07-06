package com.suncompass.tool.sz.sync.util;

import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class DateUtil {
    private static final String dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String pattern = "\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z";

    public static LocalDateTime parse(Object dateStr) {
        if (StringUtils.isEmpty(dateStr)) {
            return null;
        }

        return Instant.parse(dateStr.toString()).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * mysql date
     * @param dateStr
     * @return
     */
    public static LocalDateTime parseDateField(Object dateStr) {
        if (StringUtils.isEmpty(dateStr)) {
            return null;
        }

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0");
        return LocalDateTime.from(dateTimeFormatter.parse(dateStr.toString()));
    }

    public static boolean validDate(Object dateStr) {
        if (StringUtils.isEmpty(dateStr)) {
            return false;
        }

        return Pattern.matches(pattern, dateStr.toString());
    }
}
