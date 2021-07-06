package com.suncompass.tool.sz.sync;

import com.alibaba.fastjson.JSONObject;
import com.suncompass.tool.dbsync.canal.CanalClientJobTask;
import com.suncompass.tool.dbsync.config.CanalConfiguration;
import com.suncompass.tool.dbsync.data.CommonMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * RestCanalJobTask
 *
 * @author dengbq
 * @since 2020/6/23
 */
//@Component
public class RestCanalJobTask extends CanalClientJobTask {
    private final Logger logger = LoggerFactory.getLogger(RestCanalJobTask.class);
    private final RestTemplate restTemplate;
    @Value("${upload.url:''}")
    private String uploadUrl;

    public RestCanalJobTask(CanalConfiguration configuration) {
        super(configuration);
        restTemplate = new RestTemplate();
    }

    @Override
    protected void onInsert(List<CommonMessage> messages) {
        messages.forEach(m -> {
            logger.info("insert:" + m.getTable());

            m.getData().forEach(row -> {
                String url = String.format("%s/%s/add", uploadUrl, m.getTable());
                MultiValueMap<String, String> values = new LinkedMultiValueMap<>();
                row.entrySet().stream().forEach(e->values.add(e.getKey(),e.getValue().toString()));
                HttpHeaders headers = new HttpHeaders();
                headers.add("Content-Type", "application/json");
                HttpEntity httpEntity = new HttpEntity<>(JSONObject.toJSONString(row), headers);
                ResponseEntity<Map> res = restTemplate.postForEntity(url, httpEntity, Map.class);

            });

        });
    }

    @Override
    protected void onDelete(List<CommonMessage> messages) {

    }

    @Override
    protected void onUpdate(List<CommonMessage> messages) {
        logger.info("thread name [{}- id{}] -更新数据",Thread.currentThread().getName(), Thread.currentThread().getId());
    }

}
