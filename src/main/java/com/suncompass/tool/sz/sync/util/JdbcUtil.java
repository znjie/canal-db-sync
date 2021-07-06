package com.suncompass.tool.sz.sync.util;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class JdbcUtil {
    public static Map<String, Object> queryForMap(JdbcTemplate jdbcTemplate, String sql, Object... args) {
        Map<String, Object> resultMap = null;
        try {
            resultMap = jdbcTemplate.queryForMap(sql, args);
        } catch (EmptyResultDataAccessException ex) {
            //ex.printStackTrace();
        } catch (Exception ex) {
            throw ex;
        }

        return resultMap;
    }

    public static void insert(JdbcTemplate jdbcTemplate, String sql, JSONObject jsonObject) {
        String columns = sql.substring(sql.indexOf("(") + 1, sql.indexOf(")"));
        List<Object> argList = new LinkedList<>();
        for (String column : columns.split("\\,")) {
            column = column.trim();
            if (column.indexOf("`") >= 0) {
                column = column.replaceAll("\\`", "");
            }

            column = StrUtil.toCamelCase(column);
            if (jsonObject.containsKey(column)) {
                Object value = jsonObject.get(column);
                if (DateUtil.validDate(value)) {
                    argList.add(DateUtil.parse(value));
                } else {
                    argList.add(value);
                }
            }
        }

        jdbcTemplate.update(sql, argList.toArray());
    }

    public static String buildInsertSql(String tableName, List<String> columnList) {
        String columns =Joiner.on(",").skipNulls().join(columnList);
        String params = Joiner.on(",").skipNulls().join(columnList.stream().map(k->":"+k.trim()).toArray());
        String sql = String.format("insert into %s (%s) values (%s)", tableName, columns, params);
        return  sql;
    }

    public static String buildUpdateSql(String tableName, List<String> columnList, Object id) {
        String updateStatement = Joiner.on(",").skipNulls().join(columnList.stream().map(k->k.trim()+"=:"+k.trim()).toArray());
        String sql = String.format("update %s set %s where id='%s'", tableName, updateStatement, id);
        return  sql;
    }

    public static String buildSaveSql(String tableName, List<String> columnList) {
        String columns =Joiner.on(",").skipNulls().join(columnList);
        String params = Joiner.on(",").skipNulls().join(columnList.stream().map(k->":"+k.trim()).toArray());
        String sql = String.format("replace into %s (%s) values (%s)", tableName, columns, params);
        return  sql;
    }

}
