package com.suncompass.tool.sz.sync.task;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpGlobalConfig;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.Method;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.suncompass.tool.dbsync.canal.CanalClientJobTask;
import com.suncompass.tool.dbsync.config.CanalConfiguration;
import com.suncompass.tool.dbsync.data.CommonMessage;
import com.suncompass.tool.sz.sync.util.Constants;
import com.suncompass.tool.sz.sync.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@Component
public class SzCanalTask extends CanalClientJobTask {
    private final Logger log = LoggerFactory.getLogger(SzCanalTask.class);

    @Autowired
    private RedisUtil redisUtil;

    @Value("${ProEmergency.prefix}")
    private String prefix;

    private final String dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private ObjectMapper objectMapper = new ObjectMapper();

    public SzCanalTask(CanalConfiguration canalConfiguration) {
        super(canalConfiguration);

        JavaTimeModule javaTimeModule = new JavaTimeModule();
        objectMapper.registerModule(javaTimeModule);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.setTimeZone(TimeZone.getTimeZone("GMT-00:00"));
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

    @Override
    protected void onInsert(List<CommonMessage> messages) {
        messages.forEach(m -> {
            String controller = StrUtil.toCamelCase(m.getTable());
            String url = String.format("%s/%s/insert", prefix, controller);
            log.info("开始上报新数据-{}", url);
            String tableName = m.getTable();

            for (Map<String, Object> data : m.getData()) {
                String recordKey = Constants.recordTimestampKey(tableName, data);
                log.info("上报数据时key值："+recordKey);
                if (recordExist(recordKey)) {
                    log.info("已从省更新下来，不能再上不报-{}", data);
                    continue;
                }

                Map<String, Object> pdata = MapUtil.toCamelCaseMap(data);
                String body = null;
                try {
                    body = objectMapper.writeValueAsString(pdata);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                String result = HttpUtil.post(url, body);
                redisUtil.set(recordKey, 1, Constants.RECORD_KEY_EXPIRE_TIME);

                log.info(String.format("post url:【%s】,data:【%s】,result:【%s】", url, body, result));
                if ("baseAttachment".equals(controller)) {
                    log.info("需要上传的附件-{}", data.get("path"));
                    if (!redisUtil.lRightPush(Constants.FileUploadKey, data.get("path"))) {
                        log.error("添加附件失败-{}", data.get("path"));
                    }
                }
            }
        });
    }

    @Override
    protected void onDelete(List<CommonMessage> messages) {
        messages.forEach(m -> {
            String controller = StrUtil.toCamelCase(m.getTable());
            for (Map<String, Object> data : m.getData()) {
                String url = String.format("%s/%s/delete?id=%s", prefix, controller, data.get("id"));
                String result = HttpUtil.createRequest(Method.DELETE, url).timeout(HttpGlobalConfig.getTimeout()).execute().body();
                log.info(String.format("delete url:【%s】,result:【%s】", url, result));
            }
        });
    }

    @Override
    protected void onUpdate(List<CommonMessage> messages) {
        messages.forEach(m -> {
            String controller = StrUtil.toCamelCase(m.getTable());
            String url = String.format("%s/%s/update", prefix, controller);
            log.info("开始更新数据-{}", url);
            String tableName = m.getTable();
            for (Map<String, Object> data : m.getData()) {
                String recordKey = Constants.recordTimestampKey(tableName, data);
                log.info("更新数据时key值："+recordKey);
                if (recordExist(recordKey)) {
                    log.info("已从省更新下来，不能再上报-{}", data);
                    continue;
                }

                Map<String, Object> pdata = MapUtil.toCamelCaseMap(data);
                String body = null;
                try {
                    body = objectMapper.writeValueAsString(pdata);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                String result = HttpUtil.createRequest(Method.PUT, url).timeout(HttpGlobalConfig.getTimeout()).body(body).execute().body();
                redisUtil.set(recordKey, 1, Constants.RECORD_KEY_EXPIRE_TIME);

                log.info(String.format("put url:【%s】,data:【%s】,result:【%s】", url, body, result));
            }
        });
    }
}
