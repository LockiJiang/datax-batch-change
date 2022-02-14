package com.locki.datax.batch.change.dao;

import com.locki.datax.batch.change.utils.ConnectionUtil;
import com.locki.datax.batch.change.vo.JobsSearchVo;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务处理持久化
 *
 * @author jiangyang
 * @date 2022/2/14
 */
@Component
public class JobInfoDao {
    /**
     * 查询datax任务
     *
     * @param vo
     * @return {@link List<Map<String, String>>}
     * @author jiangyang
     * @date 2022/2/14
     */
    public List<Map<String, String>> listJobs(JobsSearchVo vo) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();

        String jobDesc = vo.getJobDesc();
        String projectName = vo.getProjectName();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuffer sb = new StringBuffer();
        sb.append(" SELECT j.id, j.job_desc, COALESCE(p.`name`, j.project_id) as project_name, j.job_cron, j.executor_route_strategy");
        sb.append(" FROM job_info j left join job_project p on j.project_id = p.id");
        sb.append(" WHERE glue_type = 'DATAX'");
        if (jobDesc != null && jobDesc.trim().length() > 0) {
            sb.append(" AND j.job_desc LIKE ?");
        }
        if (projectName != null && projectName.trim().length() > 0) {
            sb.append(" AND p.`name` LIKE ?");
        }
        try {
            con = ConnectionUtil.getCon(vo.getDbHost(), vo.getDbPort(), vo.getDbName(), vo.getDbUsername(), vo.getDbPassword());
            ps = con.prepareStatement(sb.toString());
            int i = 0;
            if (jobDesc != null && jobDesc.trim().length() > 0) {
                i++;
                ps.setString(i, "%" + jobDesc + "%");
            }
            if (projectName != null && projectName.trim().length() > 0) {
                i++;
                ps.setString(i, "%" + projectName + "%");
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                map.put("id", rs.getString("id"));
                map.put("job_desc", rs.getString("job_desc"));
                map.put("project_name", rs.getString("project_name"));
                map.put("job_cron", rs.getString("job_cron"));
                map.put("executor_route_strategy", rs.getString("executor_route_strategy"));
                list.add(map);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            ConnectionUtil.close(con, ps, rs);
        }
        return list;
    }

    /**
     * 查询任务信息
     *
     * @param vo
     * @return {@link List <Map<String, String>>}
     * @author jiangyang
     * @date 2020/11/12
     */
    public List<Map<String, String>> getData(JobsSearchVo vo) {
        List<Map<String, String>> list = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionUtil.getCon(vo.getDbHost(), vo.getDbPort(), vo.getDbName(), vo.getDbUsername(), vo.getDbPassword());
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT * FROM job_info WHERE id in (");
            for (Integer id : vo.getIds()) {
                sb.append(id).append(",");
            }
            sb.append("-1)");
            ps = con.prepareStatement(sb.toString());
            rs = ps.executeQuery();
            while (rs != null && rs.next()) {
                Map<String, String> map = new HashMap<>();
                map.put("id", rs.getString("id"));
                map.put("job_group", rs.getString("job_group"));
                map.put("job_cron", rs.getString("job_cron"));
                map.put("job_desc", rs.getString("job_desc"));
                map.put("project_id", rs.getString("project_id"));
                map.put("add_time", rs.getString("add_time"));
                map.put("update_time", rs.getString("update_time"));
                map.put("user_id", rs.getString("user_id"));
                map.put("alarm_email", rs.getString("alarm_email"));
                map.put("executor_route_strategy", rs.getString("executor_route_strategy"));
                map.put("executor_handler", rs.getString("executor_handler"));
                map.put("executor_param", rs.getString("executor_param"));
                map.put("executor_block_strategy", rs.getString("executor_block_strategy"));
                map.put("executor_timeout", rs.getString("executor_timeout"));
                map.put("executor_fail_retry_count", rs.getString("executor_fail_retry_count"));
                map.put("glue_type", rs.getString("glue_type"));
                map.put("glue_source", rs.getString("glue_source"));
                map.put("glue_remark", rs.getString("glue_remark"));
                map.put("glue_updatetime", rs.getString("glue_updatetime"));
                map.put("child_jobid", rs.getString("child_jobid"));
                map.put("trigger_status", rs.getString("trigger_status"));
                map.put("trigger_last_time", rs.getString("trigger_last_time"));
                map.put("trigger_next_time", rs.getString("trigger_next_time"));
                map.put("job_json", rs.getString("job_json"));
                map.put("replace_param", rs.getString("replace_param"));
                map.put("jvm_param", rs.getString("jvm_param"));
                map.put("inc_start_time", rs.getString("inc_start_time"));
                map.put("partition_info", rs.getString("partition_info"));
                map.put("last_handle_code", rs.getString("last_handle_code"));
                map.put("replace_param_type", rs.getString("replace_param_type"));
                map.put("reader_table", rs.getString("reader_table"));
                map.put("primary_key", rs.getString("primary_key"));
                map.put("inc_start_id", rs.getString("inc_start_id"));
                map.put("increment_type", rs.getString("increment_type"));
                map.put("datasource_id", rs.getString("datasource_id"));
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.close(con, ps, rs);
        }
        return list;
    }

    /**
     * 保存json
     *
     * @param vo
     * @param list
     * @return
     * @author jiangyang
     * @date 2022/2/14
     */
    public void updateJobJson(JobsSearchVo vo, List<Map<String, String>> list) {
        if (list == null || list.size() < 1) {
            return;
        }
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionUtil.getCon(vo.getDbHost(), vo.getDbPort(), vo.getDbName(), vo.getDbUsername(), vo.getDbPassword());
            String upt = "update job_info set job_json = ? where id = ?";
            con.setAutoCommit(false);
            ps = con.prepareStatement(upt);
            for (Map<String, String> map : list) {
                ps.setString(1, map.get("job_json"));
                ps.setString(2, map.get("id"));
                ps.addBatch();
            }
            ps.executeBatch();
            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.close(con, ps, null);
        }
    }
}
