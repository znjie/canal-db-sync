package com.suncompass.tool.sz.sync.util;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface Constants {
    String FileUploadKey = "fileUpload";
    String FileDownloadKey = "fileDownload";

    String CanalField = "alicanal";
    String CanalExclude = "exclude";

    String DB_ID_FIELD = "id";
    String DB_CREATED_FIELD = "created";
    String DB_UPDATED_FIELD = "updated";
    String DELETE_FLAG = "del-";
    String INIT_FLAG = "init-";
    String TASK_INIT_FLAG = "TASK-INIT-KEY";
    String LOCAL_CACHE_KEY = "CACHE-INIT-RECORD";

    String ATTACHMENT_TABLE_NAME = "base_attachment";

    String PACKAGE_PREFIX = "com.suncompass.huanjinyingji";

    long RECORD_KEY_EXPIRE_TIME = 60 * 60 * 24 * 1;

    ConcurrentHashMap<String, Object> LocalCacheKeys = new ConcurrentHashMap<>(100);

    static String buildDelKey(String tableName) {
        return DELETE_FLAG + tableName;
    }

    static String recordTimestampKey(String prefix, Map record) {
        Object id = record.get(Constants.DB_ID_FIELD);
        Object updated = record.get(Constants.DB_UPDATED_FIELD);
        Object created = record.get(Constants.DB_CREATED_FIELD);
        if (created != null && created instanceof String) {
            created = Instant.parse(created.toString());
        }

        if (updated != null && updated instanceof String) {
            updated = Instant.parse(updated.toString());
        }

        Instant lastCreated = created != null ? (Instant) created : Instant.EPOCH;
        Instant lastUpdated = updated != null ? (Instant) updated : Instant.EPOCH;
        BigDecimal key = new BigDecimal(lastCreated.getEpochSecond()).add(new BigDecimal(lastUpdated.getEpochSecond()));

        return String.format("rid_%s_%s_%s", prefix, id, key.longValue());
    }
}