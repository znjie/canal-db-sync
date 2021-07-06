package com.suncompass.tool.sz.sync.task;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class SzDownloadFileTask extends AbstractJobTask {
    private Logger log = LoggerFactory.getLogger(SzDownloadFileTask.class);

    @Autowired
    private RedisUtil redisUtil;

    @Value("${ProEmergency.baseApi}")
    private String baseApi;

    @Value("${upfileDir}")
    private String upfileDir;

    @Value(value = "${task.downloadFile:0 1/10 * * * ?}")
    private String cron;

    private static final int maxSize = 20;
    private static final ExecutorService executorService = new ThreadPoolExecutor(
            4,
            maxSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024),
            new ThreadFactoryBuilder().setNameFormat("download-pool-%d").build(),
            new ThreadPoolExecutor.AbortPolicy()
    );

    @Override
    protected void doTask() {
        log.info("Start DownloadFile Task");

        String downloadFile = baseApi + "/attachment/download?id={0}";

        List<Object> list = redisUtil.lRange(Constants.FileDownloadKey, 0, maxSize);
        if (CollUtil.isEmpty(list)) {
            log.info("list is empty");
            return;
        }

        for (Object object : list) {
            //JSONObject jsonAttachment = (JSONObject) object;
            JSONObject jsonAttachment = (JSONObject) JSON.toJSON(object);
            String id = jsonAttachment.get("id").toString();
            String path = jsonAttachment.get("path").toString();
            String url = MessageFormat.format(downloadFile, id);
            String filePath = upfileDir + path;
            File destFile = FileUtil.file(filePath);
            if (destFile.exists()) {
                // 如果目标文件已存在，跳过
                log.info("文件已存在-{}", destFile);
                redisUtil.lRemove(Constants.FileDownloadKey, 1, path);
                continue;
            }

            if (!destFile.getParentFile().exists()) {
                // 创建目录
                destFile.getParentFile().mkdirs();
            }

            // 线程池执行
            executorService.execute(() -> {
                HttpUtil.downloadFile(url, destFile);
                redisUtil.lRemove(Constants.FileDownloadKey, 1, path);
                log.info(String.format("download url:【%s】,path:【%s】", url, filePath));
            });
        }
    }

    @Override
    public String getCron() {
        return cron;
    }
}
