package com.suncompass.tool.sz.sync.task;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.suncompass.tool.dbsync.task.AbstractJobTask;
import com.suncompass.tool.sz.sync.util.Constants;
import com.suncompass.tool.sz.sync.util.JdbcUtil;
import com.suncompass.tool.sz.sync.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.text.MessageFormat;
import java.util.Map;

//@Component
public class SzSyncBulletinTask extends AbstractJobTask {
    private Logger log = LoggerFactory.getLogger(SzSyncBulletinTask.class);

    @Autowired
    @Qualifier("destJdbcTemplate")
    private JdbcTemplate destJdbcTemplate;

    @Autowired
    private RedisUtil redisUtil;

    @Value(value = "${task.syncNotice:0/30 * * * * ?}")
    private String cron;

    @Value("${ProEmergency.prefix}")
    private String prefix;

    final String selectAttachment = "select id from base_attachment where id = ?";
    final String insertAttachment = "insert into base_attachment(id,created,file_name,ext,path,file_size,md5,`" + Constants.CanalField + "`) values(?,?,?,?,?,?,?,'" + Constants.CanalExclude + "')";

    final String selectBulletin = "select id from base_bulletin where id = ?";
    final String insertBulletin = "insert into base_bulletin(id,created,updated,attachment_id,content,priority,`status`,title,`type`,source,organization_id,industry_code,risk_level) values(?,?,?,?,?,?,?,?,?,?,?,?,?)";

    final String insertBulletinObject = "insert into base_bulletin_object(id,created,bulletin_id,organization_id) values(?,?,?,?)";

    @Override
    protected void doTask() {
        log.info("Start SyncBulletin Task...");
        syncBulletin();
        log.info("SyncBulletin Task Complete!");
    }

    /**
     * 获取省厅对深圳区域的通知公告
     */
    private void syncBulletin() {
        String getBulletinList = prefix + "/baseBulletin/getBulletinList?prefixOrgCode={0}&publishOrgCode={1}";
        String strBulletins = HttpUtil.get(MessageFormat.format(getBulletinList, "003003003", "003003"));
        JSONArray jsonBulletins = (JSONArray) JSON.parse(strBulletins);

        log.info("通知公告数-{}", jsonBulletins.size());

        for (int i = 0, il = jsonBulletins.size(); i < il; i++) {
            JSONObject jsonBulletin = (JSONObject) jsonBulletins.get(i);

            Map<String, Object> bulletinMap = JdbcUtil.queryForMap(destJdbcTemplate, selectBulletin, jsonBulletin.get("id").toString());
            if (bulletinMap != null && bulletinMap.get("id") != null) {
                // 存在则忽略
                log.info("记录已存在-{}",  bulletinMap.get("id"));
                continue;
            }

            if (jsonBulletin.get("attachmentId") != null) {
                pushDownload(jsonBulletin.get("attachmentId").toString());
            }

            JdbcUtil.insert(destJdbcTemplate, insertBulletin, jsonBulletin);
            JSONArray jsonBulletinObjects = (JSONArray) jsonBulletin.get("bulletinObjects");
            for (int j = 0, jl = jsonBulletinObjects.size(); j < jl; j++) {
                JSONObject jsonBulletinObject = (JSONObject) jsonBulletinObjects.get(j);
                JdbcUtil.insert(destJdbcTemplate, insertBulletinObject, jsonBulletinObject);
            }
        }
    }

    private void pushDownload(String attachmentId) {

        Map<String, Object> attachmentMap = JdbcUtil.queryForMap(destJdbcTemplate, selectAttachment, attachmentId);
        if (attachmentMap == null || StringUtils.isEmpty(attachmentMap.get("id"))) {
            String getAttachment = prefix + "/baseAttachment/get?id={0}";
            String attachmentResult = HttpUtil.get(MessageFormat.format(getAttachment, attachmentId));
            log.info("插入通知公告附件-{}", attachmentId);

            JSONObject jsonAttachment = (JSONObject) JSON.parse(attachmentResult);
            JdbcUtil.insert(destJdbcTemplate, insertAttachment, jsonAttachment);
            redisUtil.lRightPush(Constants.FileDownloadKey, jsonAttachment);
        }
    }

    @Override
    public String getCron() {
        return cron;
    }
}
