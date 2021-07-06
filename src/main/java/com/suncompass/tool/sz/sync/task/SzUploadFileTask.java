package com.suncompass.tool.sz.sync.task;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.http.HttpUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.suncompass.tool.dbsync.task.AbstractJobTask;
import com.suncompass.tool.sz.sync.util.Constants;
import com.suncompass.tool.sz.sync.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class SzUploadFileTask extends AbstractJobTask {
    private Logger log = LoggerFactory.getLogger(SzUploadFileTask.class);

    @Autowired
    private RedisUtil redisUtil;

    @Value("${ProEmergency.baseApi}")
    private String baseApi;

    @Value("${upfileDir}")
    private String upfileDir;

    @Value(value = "${task.uploadFile:0 1/10 * * * ?}")
    private String cron;

    private static final int maxSize = 20;
    private static final ExecutorService executorService = new ThreadPoolExecutor(
            4,
            maxSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024),
            new ThreadFactoryBuilder().setNameFormat("upload-pool-%d").build(),
            new ThreadPoolExecutor.AbortPolicy()
    );

    @Override
    protected void doTask() {
        log.info("Start UploadFile Task");

        String fileUpload = baseApi + "/attachment/fileUpload";

        List<Object> list = redisUtil.lRange(Constants.FileUploadKey, 0, maxSize);
        if (CollUtil.isEmpty(list)) {
            log.info("list is empty");
            return;
        }

        for (Object object : list) {
            String path = object.toString();
            Map<String, Object> paramMap = new HashMap<>();
            String filePath = upfileDir + path;
            if (!new File(filePath).exists()) {
                log.info("文件不存在-{}", filePath);
                redisUtil.lRemove(Constants.FileUploadKey, 1, path);
                continue;
            }
            paramMap.put("fileName", FileUtil.file(filePath));
            paramMap.put("path", path);
            // 线程池执行
            executorService.execute(() -> {
                String responseText = HttpUtil.post(fileUpload, paramMap);
                redisUtil.lRemove(Constants.FileUploadKey, 1, path);
                log.info(String.format("upload url:【%s】,path:【%s】,result:【%s】", fileUpload, filePath, responseText));
            });
        }
    }

    @Override
    public String getCron() {
        return cron;
    }
}
