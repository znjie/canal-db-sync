package com.suncompass.tool.sz.sync.task;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.suncompass.tool.dbsync.canal.util.JdbcTypeUtil;
import com.suncompass.tool.dbsync.task.AbstractJobTask;
import com.suncompass.tool.sz.sync.util.Constants;
import com.suncompass.tool.sz.sync.util.JdbcUtil;
import com.suncompass.tool.sz.sync.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DownDataTask
 *
 * @author dengbq
 * @since 2020/9/14
 */
@Component
public class DownDataTask extends AbstractJobTask {
    @Value(value = "${task.downData:0 1/1 * * * ?}")
    private String cron;

    @Value("${ProEmergency.downloadUrl:'blank url'}")
    private String downUrl;

    @Value("${task.tables}")
    private List<String> tables;

    @Autowired
    private RestTemplate template;

    @Autowired
    @Qualifier("destJdbcTemplate")
    private JdbcTemplate destJdbcTemplate;

    @Autowired
    private RedisUtil redisUtil;

    private Logger log = LoggerFactory.getLogger(DownDataTask.class);

    @Override
    protected void doTask() {
        Assert.notNull(tables, "同步表不能为空");

        tables.forEach(t -> {
            List<Map> data = download(t);
            save(t, data);
        });
        InitCache();
        boolean b = Optional.ofNullable(redisUtil.get(Constants.TASK_INIT_FLAG)).map(v -> (int) v != 0).orElse(false);
        if (b) {
            tables.forEach(t -> {
                List<Map> data = download(t, String.format("%s/%s?tableName=%s", downUrl, "getAddOrUpdate", Constants.INIT_FLAG + t));
                batchInit(t, data);
            });
            redisUtil.set(Constants.TASK_INIT_FLAG, 0);
        }
    }

    @Override
    public String getCron() {
        return cron;
    }

    private List<Map> download(String t) {
        String url = String.format("%s/%s?tableName=%s", downUrl, "getAddOrUpdate", t);
        return download(t, url);
    }

    private List<Map> download(String t, String url) {
        try {
            ResponseEntity resp = template.getForEntity(url, List.class);

            List<Map> data = (List<Map>) resp.getBody();
            log.info("获取更新数据{}-{}条", t, data.size());
            return data;
        } catch (RestClientException e) {
            log.error("下载表[{}]数据失败:{}", t, e.getMessage());
            return null;
        }
    }

    private void save(String t, List<Map> mapList) {
        SqlRowSetMetaData tableMeta = null;

        if (mapList == null || mapList.size() == 0) return;

        tableMeta = getTableMeta(t);
        List<String> columnNames = null;

        for (int i = 0; i < mapList.size(); i++) {
            final Map d = mapList.get(i);

            if (i == 0) columnNames = mergeColumns(tableMeta, d);
            if (columnNames == null || columnNames.size() == 0) {
                log.error("无法获取到列名数据:{}", t);
                return;
            }

            String recordKey = Constants.recordTimestampKey(t, d);
            if (recordExist(recordKey)) {
                log.debug("数据已存在:{}", d);
                continue;
            } else {
                log.debug("保存数据:{}", d);
                Map<String, Object> rowObject = mergeData(tableMeta, d);
                save(t, rowObject, columnNames);
                attachmentDown(t, rowObject);
                Constants.LocalCacheKeys.put(recordKey, 1);
                redisUtil.set(recordKey, 1, Constants.RECORD_KEY_EXPIRE_TIME);
            }
        }
    }

    private void save(String table, Map row, List<String> columns) {
        String sql = null;

        try {
            if (!recordExist(table, row)) {
                sql = JdbcUtil.buildInsertSql(table, columns);
            } else {
                sql = JdbcUtil.buildUpdateSql(table, columns, row.get("id"));
            }

            log.debug(sql);
            NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(destJdbcTemplate);
            namedParameterJdbcTemplate.update(sql, row);
        } catch (DataAccessException e) {
            log.error("保存表[{}]数据[{}]失败:{}", table, row, e.getMessage());
        }
    }

    private void attachmentDown(String tableName, Map row){
        if (Constants.ATTACHMENT_TABLE_NAME.equals(tableName)) {
            try {
                log.info("需要下载附件-{}", row.get("path"));
                if (!redisUtil.lRightPush(Constants.FileDownloadKey, row)) {
                    log.error("添加附件失败-{}", row.get("path"));
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    private void batchInit(String t, List<Map> mapList) {
        if (mapList == null || mapList.size() == 0) return;

        try {
            final SqlRowSetMetaData tableMeta = getTableMeta(t);

            List<String> columnNames = mergeColumns(tableMeta, mapList.get(0));
            List<Map<String, Object>> newDataList = mapList.stream().map(d -> {
                String recordKey = Constants.recordTimestampKey(t, d);
                Constants.LocalCacheKeys.put(recordKey, 1);
                attachmentDown(t, d);
                return mergeData(tableMeta, d);
            }).collect(Collectors.toList());

            Map<String, Object>[] dataArray = new HashMap[]{};
            dataArray = newDataList.toArray(dataArray);

            if (columnNames == null || columnNames.size() == 0) {
                log.error("无法获取到列名数据:{}", t);
                return;
            }

            //批量插入，重复数据将删除+插入
            String sql = JdbcUtil.buildSaveSql(t, columnNames);
            log.debug(sql);
            NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(destJdbcTemplate);
            namedParameterJdbcTemplate.batchUpdate(sql, dataArray);
        } catch (Exception e) {
            log.error("批量初始数据表:{}失败-{}", t, e.getMessage());
        }
    }

    private String getEntityNameFromTable(String tableName) {
        tableName = tableName.replace("base_", "");
        if (tableName.equals("process_opinion")) {
            tableName = "opinion";
        }
        return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, tableName);
    }

    private Class<?> getClass(String name) {
        try {
            ImmutableSet<ClassPath.ClassInfo> clazzSet = ClassPath.from(ClassLoader.getSystemClassLoader()).getTopLevelClassesRecursive(Constants.PACKAGE_PREFIX);
            for (ClassPath.ClassInfo ci : clazzSet) {
                if (ci.getName().endsWith(name)) {
                    return ci.load();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private SqlRowSetMetaData getTableMeta(String table) {
        try {
            SqlRowSet sqlRowSet = destJdbcTemplate.queryForRowSet(String.format("select * from %s limit 0", table));
            return sqlRowSet.getMetaData();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<String> mergeColumns(SqlRowSetMetaData metaData, Map row) {
        List<String> mergeColumns = new LinkedList<>();
        try {
            int count = metaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                if (row.containsKey(metaData.getColumnName(i))) {
                    mergeColumns.add(metaData.getColumnName(i));
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return mergeColumns;
    }

    private Map<String, Object> mergeData(SqlRowSetMetaData metaData, Map row) {
        Map<String, Object> datas = new HashMap<>();
        try {
            int count = metaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                if (row.containsKey(metaData.getColumnName(i))) {
                    Object v = row.get(metaData.getColumnName(i));
                    if (v != null && v.toString().matches("^\\d{4}\\S+T\\S+Z$")) {
                        Instant instant = Instant.parse(v.toString());
                        Timestamp timestamp = Timestamp.from(instant);
                        datas.put(metaData.getColumnName(i), timestamp);
                        continue;
                    }
                    datas.put(metaData.getColumnName(i), JdbcTypeUtil.typeConvert(metaData.getTableName(i), metaData.getColumnName(i), v == null ? null : v.toString(), metaData.getColumnType(i), metaData.getColumnTypeName(i)));
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return datas;
    }

    private boolean recordExist(String tableName, Map row) {
        Object id = row.get("id");
        int rn = destJdbcTemplate.queryForObject(String.format("select count(1) from %s where id='%s'", tableName, id), Integer.class);

        return rn > 0;
    }

    /**
     * 记录是否已存在
     *
     * @param key
     * @return
     */
    private boolean recordExist(String key) {
        return Constants.LocalCacheKeys.contains(key) || redisUtil.hasKey(key);
    }

    private void InitCache() {
        if (Constants.LocalCacheKeys.size() == 0) {
            Map<Object, Object> values = redisUtil.hmget(Constants.LOCAL_CACHE_KEY);
            if (values != null) {
                values.entrySet().forEach(e -> {
                    Constants.LocalCacheKeys.put(e.getKey().toString(), e.getValue());
                });
            }
        } else {
            redisUtil.hmset(Constants.LOCAL_CACHE_KEY, Constants.LocalCacheKeys);
        }
    }
}
