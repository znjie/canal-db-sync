package com.suncompass.tool.sz.sync.task;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.suncompass.tool.dbsync.task.AbstractJobTask;
import com.suncompass.tool.sz.sync.util.Constants;
import com.suncompass.tool.sz.sync.util.DateUtil;
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
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

//@Component
public class SzSyncFilingTask extends AbstractJobTask {
    private Logger log = LoggerFactory.getLogger(SzSyncFilingTask.class);

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

    final String selectFiling = "select * from emergency_filing where id = ?";
    final String updateFiling = "update emergency_filing set updated = ?, approve_status = ?, `year` = ?, filing_time = ?, filing_no = ?, area_code = ?, serial_number = ?, unit_code = ?, unit_name = ?, approve_reason = ?, approve_file_id = ?, principal = ?, operater = ?, registration_file_id = ?, `" + Constants.CanalField + "` = '" + Constants.CanalExclude + "' where id = ?";

    final String updateRiskSource = "update base_risk_source set risk_level = ?, `" + Constants.CanalField + "` = '" + Constants.CanalExclude + "' where id = ?";

    final String updateProcessInstance = "update process_instance set updated = ?, finished = ?, finished_time = ?, current_node_id = ?, agent = ?, `" + Constants.CanalField + "` = '" + Constants.CanalExclude + "' where id = ?";

    final String selectProcessOpinionList = "select id from process_opinion where instance_id = ?";
    final String insertProcessOpinion = "insert into process_opinion(id,created,user_id,opinion,node,instance_id,`action`,`" + Constants.CanalField + "`) values(?,?,?,?,?,?,?,'" + Constants.CanalExclude + "')";

    @Override
    protected void doTask() {
        log.info("Start SyncFiling Task...");
        syncFiling();
        log.info("SyncFiling Task Complete!");
    }

    /**
     * 获取省厅对深圳企业审核的备案
     */
    private void syncFiling() {
        String getFinishedList = prefix + "/emergencyFiling/getFinishedList?prefixEntCode={0}&agentCode={1}";
        String strFilings = HttpUtil.get(MessageFormat.format(getFinishedList, "003003003", "003003"));
        JSONArray jsonFilings = (JSONArray) JSON.parse(strFilings);
        log.info("备案数-{}", jsonFilings.size());

        for (int i = 0, il = jsonFilings.size(); i < il; i++) {
            JSONObject jsonFiling = (JSONObject) jsonFilings.get(i);
            Map<String, Object> filingMap = JdbcUtil.queryForMap(destJdbcTemplate, selectFiling, jsonFiling.get("id").toString());

            LocalDateTime srcUpdate = DateUtil.parse(jsonFiling.get("updated"));
            LocalDateTime descUpdate = DateUtil.parseDateField(filingMap.get("updated"));

            if (srcUpdate!=null && (srcUpdate.isBefore(descUpdate) || srcUpdate.isEqual(descUpdate))) {
                log.info("备案更新时间早于本地，跳过更新：id={}, lt={},rt={}", jsonFiling.get("id").toString(), filingMap.get("updated"), jsonFiling.get("updated"));
                continue;
            }

            log.info("更新备案信息-{}", jsonFiling);

            if (jsonFiling.get("approveFileId") != null) {
                // 审核附件
                pushDownload(jsonFiling.get("approveFileId").toString());
            }

            if (jsonFiling.get("registrationFileId") != null) {
                // 环保局上传的备案表
                pushDownload(jsonFiling.get("registrationFileId").toString());
            }

            destJdbcTemplate.update(updateFiling,
                    DateUtil.parse(jsonFiling.get("updated")),
                    jsonFiling.get("approveStatus"),
                    jsonFiling.get("year"),
                    DateUtil.parse(jsonFiling.get("filingTime")),
                    jsonFiling.get("filingNo"),
                    jsonFiling.get("areaCode"),
                    jsonFiling.get("serialNumber"),
                    jsonFiling.get("unitCode"),
                    jsonFiling.get("unitName"),
                    jsonFiling.get("approveReason"),
                    jsonFiling.get("approveFileId"),
                    jsonFiling.get("principal"),
                    jsonFiling.get("operater"),
                    jsonFiling.get("registrationFileId"),
                    jsonFiling.get("id"));

            JSONObject jsonRiskSource = (JSONObject) jsonFiling.get("curRiskSource");
            destJdbcTemplate.update(updateRiskSource,
                    jsonRiskSource.get("riskLevel"),
                    jsonRiskSource.get("id"));

            JSONObject jsonProcessInstance = (JSONObject) jsonFiling.get("processInstance");
            destJdbcTemplate.update(updateProcessInstance,
                    DateUtil.parse(jsonProcessInstance.get("updated")),
                    jsonProcessInstance.get("finished"),
                    DateUtil.parse(jsonProcessInstance.get("finishedTime")),
                    jsonProcessInstance.get("currentNodeId"),
                    jsonProcessInstance.get("agent"),
                    jsonProcessInstance.get("id"));

            JSONArray jsonProcessOpinions = (JSONArray) jsonFiling.get("processOpinions");
            List<Map<String, Object>> processOpinionList = destJdbcTemplate.queryForList(selectProcessOpinionList, jsonProcessInstance.get("id").toString());
            for (int j = 0, jl = jsonProcessOpinions.size(); j < jl; j++) {
                JSONObject jsonProcessOpinion = (JSONObject) jsonProcessOpinions.get(j);
                String processOpinionId = jsonProcessOpinion.get("id").toString();
                Optional<Map<String, Object>> optional = processOpinionList.stream().filter(x -> processOpinionId.equals(x.get("id").toString())).findFirst();
                if (!optional.isPresent()) {
                    JdbcUtil.insert(destJdbcTemplate, insertProcessOpinion, jsonProcessOpinion);
                }
            }
        }
    }

    private void pushDownload(String attachmentId) {
        Map<String, Object> attachmentMap = JdbcUtil.queryForMap(destJdbcTemplate, selectAttachment, attachmentId);
        if (attachmentMap == null || StringUtils.isEmpty(attachmentMap.get("id"))) {
            String getAttachment = prefix + "/baseAttachment/get?id={0}";
            String attachmentResult = HttpUtil.get(MessageFormat.format(getAttachment, attachmentId));
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
