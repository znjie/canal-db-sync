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
public class SzSyncCheckupTask extends AbstractJobTask {
    private Logger log = LoggerFactory.getLogger(SzSyncCheckupTask.class);

    @Autowired
    @Qualifier("destJdbcTemplate")
    private JdbcTemplate destJdbcTemplate;

    @Autowired
    private RedisUtil redisUtil;

    @Value(value = "${task.dataSync:0/30 * * * * ?}")
    private String cron;

    @Value("${ProEmergency.prefix}")
    private String prefix;

    final String selectAttachment = "select id from base_attachment where id = ?";
    final String insertAttachment = "insert into base_attachment(id,created,file_name,ext,path,file_size,md5,`" + Constants.CanalField + "`) values(?,?,?,?,?,?,?,'" + Constants.CanalExclude + "')";

    final String selectCheckup = "select id from emergency_risk_checkup_table where id = ?";
    final String insertCheckup = "insert into emergency_risk_checkup_table(id,created,checkup_date,department,leader,`name`,responsible_person,risk_source_id,leader_phone,leader_post,responsible_phone,responsible_post,has_illegal,illegal_attachment,illegal_behavior,organization_id,checkup_status,finish_date,has_risk,check_attachment_id,check_type,`" + Constants.CanalField + "`) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'" + Constants.CanalExclude + "')";

    final String insertCheckupItem = "insert into emergency_risk_checkup_item(id,created,control_level,exist_risk,has_risk,memo,risk_checkup_table_id,risk_level,status,latent_risk_id,`" + Constants.CanalField + "`) values(?,?,?,?,?,?,?,?,?,?,'" + Constants.CanalExclude + "')";

    @Override
    protected void doTask() {
        log.info("Start SyncCheckup Task...");
        syncCheckup();
        log.info("SyncCheckup Task Complete!");
    }

    /**
     * 获取省厅对深圳企业的隐患排查
     */
    private void syncCheckup() {
        String getCheckupList = prefix + "/emergencyRiskCheckupTable/getCheckupList?prefixEntCode={0}&checkupOrgCode={1}";
        String strCheckups = HttpUtil.get(MessageFormat.format(getCheckupList, "003003003", "003003"));
        JSONArray jsonCheckups = (JSONArray) JSON.parse(strCheckups);

        for (int i = 0, il = jsonCheckups.size(); i < il; i++) {
            JSONObject jsonCheckup = (JSONObject) jsonCheckups.get(i);
            Map<String, Object> checkupMap = JdbcUtil.queryForMap(destJdbcTemplate, selectCheckup, jsonCheckup.get("id").toString());
            if (checkupMap != null && checkupMap.get("id") != null) {
                log.info("记录已存在-{}", checkupMap.get("id"));
                continue;
            }

            if (jsonCheckup.get("checkAttachmentId") != null) {
                // 排查材料
                String attachmentIds = jsonCheckup.get("checkAttachmentId").toString();
                for (String id : attachmentIds.split(",")) {
                    pushDownload(id);
                }
            }

            JdbcUtil.insert(destJdbcTemplate, insertCheckup, jsonCheckup);
            JSONArray jsonCheckupItems = (JSONArray) jsonCheckup.get("checkupItems");
            for (int j = 0, jl = jsonCheckupItems.size(); j < jl; j++) {
                JSONObject jsonCheckupItem = (JSONObject) jsonCheckupItems.get(j);
                JdbcUtil.insert(destJdbcTemplate, insertCheckupItem, jsonCheckupItem);
            }
        }
    }

    private void pushDownload(String attachmentId) {
        Map<String, Object> attachmentMap = JdbcUtil.queryForMap(destJdbcTemplate, selectAttachment, attachmentId);
        if (attachmentMap == null || StringUtils.isEmpty(attachmentMap.get("id"))) {
            String getAttachment = prefix + "/baseAttachment/get?id={0}";
            String attachmentResult = HttpUtil.get(MessageFormat.format(getAttachment, attachmentId));
            log.info("插入附件-{}", attachmentId);
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
