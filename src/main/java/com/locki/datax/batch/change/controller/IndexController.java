package com.locki.datax.batch.change.controller;

import com.locki.datax.batch.change.utils.Configuration;
import com.locki.datax.batch.change.utils.ConnectionUtil;
import com.locki.datax.batch.change.vo.JobsSearchVo;
import com.locki.datax.batch.change.vo.Result;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


/**
 * @author jiangyang
 * @date 2020/11/10
 */
@RestController
public class IndexController {
    /**
     * 查询任务
     *
     * @param vo
     * @return {@link Result}
     * @author jiangyang
     * @date 2020/11/12
     */
    @PostMapping("/listJobs")
    public Result listJobs(@RequestBody @Valid JobsSearchVo vo) {
        String jobDesc = vo.getJobDesc();
        String projectName = vo.getProjectName();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuffer sb = new StringBuffer();
        List<Map<String, String>> list = new ArrayList<>();
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
            return Result.fail(500, "获取任务异常！", e.getMessage());
        } finally {
            ConnectionUtil.close(con, ps, rs);
        }
        return Result.success(200, "", "").setData(list);
    }

    /**
     * 为所选任务添加truncate语句
     *
     * @param vo
     * @return {@link String}
     * @author jiangyang
     * @date 2022/2/14
     */
    @PostMapping("/addTrunc")
    public String addTrunc(@RequestBody @Valid JobsSearchVo vo) {
        List<Map<String, String>> list = getData(vo);
        if (list == null || list.size() < 1) {
            return "未找到有效的任务信息！";
        }
        for (Map<String, String> map : list) {
            Configuration conf = Configuration.from(map.get("job_json"));
            //获取表名
            String tableName = conf.getString("job.content[0].writer.parameter.connection[0].table[0]");
            //获取已有的pre
            List<String> preSqls = conf.getList("job.content[0].writer.parameter.preSql", String.class);
            if (preSqls == null) {
                preSqls = new ArrayList<>();
            }
            boolean haveTrunc = false;
            Iterator<String> it = preSqls.iterator();
            while (it.hasNext()) {
                String sql = it.next();
                if (sql == null || sql.trim().length() < 1) {
                    it.remove();
                    continue;
                }
                if (sql.trim().toLowerCase().startsWith("truncate")) {
                    haveTrunc = true;
                    break;
                }
            }
            if (!haveTrunc) {
                preSqls.add(0, "TRUNCATE TABLE " + tableName);
            }
            //更新
            conf.set("job.content[0].writer.parameter.preSql", preSqls);
            map.put("job_json", conf.toString());
        }
        saveJobJson(vo, list);
        return "处理完成";
    }

    /**
     * 生成脚本文件
     *
     * @param vo
     * @param {@link String}
     * @return
     * @author jiangyang
     * @date 2020/11/12
     */
    @PostMapping("/exportSql")
    public String exportSql(@RequestBody @Valid JobsSearchVo vo) {
        String res = null;
        List<Map<String, String>> list = getData(vo);
        //处理任务json脚本
        changeDatasource(list);
        //生成脚本文件
        try {
            res = exportFile(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 下载脚本文件
     *
     * @param uuid
     * @return
     * @author jiangyang
     * @date 2020/11/12
     */
    @PostMapping("/download")
    public void download(String uuid, HttpServletResponse response) {
        //设置响应头和客户端保存文件名
        response.setCharacterEncoding("utf-8");
        response.setContentType("multipart/form-data");
        response.setHeader("Content-Disposition", "attachment;fileName=job_info.sql");
        //激活下载操作
        OutputStream os = null;
        BufferedInputStream bis = null;
        FileInputStream fis = null;
        File file = new File(uuid);
        try {
            os = response.getOutputStream();
            byte[] buff = new byte[2048];
            // 读取filename
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            int bytesRead;
            while (-1 != (bytesRead = bis.read(buff, 0, buff.length))) {
                os.write(buff, 0, bytesRead);
                os.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (os != null) {
                    os.close();
                }
                if (bis != null) {
                    bis.close();
                }
                if (fis != null) {
                    fis.close();
                }
                file.delete();
            } catch (Exception e) {
            }
        }
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
    private void saveJobJson(JobsSearchVo vo, List<Map<String, String>> list) {
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

    /**
     * 查询任务信息
     *
     * @param vo
     * @return {@link List<Map<String, String>>}
     * @author jiangyang
     * @date 2020/11/12
     */
    private List<Map<String, String>> getData(JobsSearchVo vo) {
        List<Map<String, String>> list = new ArrayList<>();
        if (vo.getIds() == null || vo.getIds().size() < 1) {
            return list;
        }
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
            while (rs.next()) {
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
     * 处理datax任务json基本，将reader和writer的数据连接信息改为特殊信息，方便replace
     *
     * @param list
     * @return
     * @author jiangyang
     * @date 2020/11/11
     */
    private void changeDatasource(List<Map<String, String>> list) {
        if (list == null || list.size() < 1) {
            return;
        }
        for (Map<String, String> map : list) {
            Configuration conf = Configuration.from(map.get("job_json"));
            //处理reader
            conf.set("job.content[0].reader.parameter.connection[0].jdbcUrl[0]", "datax_reader_datasource_url");
            conf.set("job.content[0].reader.parameter.username", "datax_reader_username");
            conf.set("job.content[0].reader.parameter.password", "datax_reader_password");
            //处理writer
            conf.set("job.content[0].writer.parameter.connection[0].jdbcUrl", "datax_writer_datasource_url");
            conf.set("job.content[0].writer.parameter.username", "datax_writer_username");
            conf.set("job.content[0].writer.parameter.password", "datax_writer_password");
            //更新
            map.put("job_json", conf.toString());
        }
    }

    /**
     * 生成SQL脚本文件
     *
     * @param list
     * @return
     * @author jiangyang
     * @date 2020/11/11
     */
    private String exportFile(List<Map<String, String>> list) throws IOException {
        String insert = "INSERT INTO `job_info`(`id`, `job_group`, `job_cron`, `job_desc`, `project_id`, `add_time`, `update_time`, `user_id`, `alarm_email`, `executor_route_strategy`, `executor_handler`, `executor_param`, `executor_block_strategy`, `executor_timeout`, `executor_fail_retry_count`, `glue_type`, `glue_source`, `glue_remark`, `glue_updatetime`, `child_jobid`, `trigger_status`, `trigger_last_time`, `trigger_next_time`, `job_json`, `replace_param`, `jvm_param`, `inc_start_time`, `partition_info`, `last_handle_code`, `replace_param_type`, `reader_table`, `primary_key`, `inc_start_id`, `increment_type`, `datasource_id`)";
        if (list == null || list.size() < 1) {
            return null;
        }

        File file = new File(UUID.randomUUID().toString());
        file.createNewFile();
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);

        for (Map<String, String> map : list) {
            StringBuffer sb = new StringBuffer(insert);
            sb.append("VALUES (");
            sb.append(map.get("id")).append(",");
            sb.append(map.get("job_group")).append(",");
            sb.append("'").append(map.get("job_cron")).append("',");
            sb.append("'").append(map.get("job_desc")).append("',");
            sb.append(map.get("project_id")).append(",");

            sb = append(sb, map.get("add_time"));
            sb = append(sb, map.get("update_time"));

            sb.append(map.get("user_id")).append(",");

            sb = append(sb, map.get("alarm_email"));
            sb = append(sb, map.get("executor_route_strategy"));
            sb = append(sb, map.get("executor_handler"));
            sb = append(sb, map.get("executor_param"));
            sb = append(sb, map.get("executor_block_strategy"));

            sb.append(map.get("executor_timeout")).append(",");
            sb.append(map.get("executor_fail_retry_count")).append(",");
            sb.append("'").append(map.get("glue_type")).append("',");

            sb = append(sb, map.get("glue_source"));
            sb = append(sb, map.get("glue_remark"));
            sb = append(sb, map.get("glue_updatetime"));
            sb = append(sb, map.get("child_jobid"));

            sb.append(map.get("trigger_status")).append(",");
            sb.append(map.get("trigger_last_time")).append(",");
            sb.append(map.get("trigger_next_time")).append(",");

            sb = append(sb, map.get("job_json"));
            sb = append(sb, map.get("replace_param"));
            sb = append(sb, map.get("jvm_param"));
            sb = append(sb, map.get("inc_start_time"));
            sb = append(sb, map.get("partition_info"));

            sb.append(map.get("last_handle_code")).append(",");

            sb = append(sb, map.get("replace_param_type"));
            sb = append(sb, map.get("reader_table"));
            sb = append(sb, map.get("primary_key"));
            sb = append(sb, map.get("inc_start_id"));

            sb.append(map.get("increment_type")).append(",");
            sb.append(map.get("datasource_id"));
            sb.append(");\r\n");
            bw.write(sb.toString());
            bw.flush();
        }

        //生成更新语句
        bw.write("--更新数据抽取任务来源库信息\r\n");
        bw.write("/*\r\n");
        bw.write("update job_info set job_json = REPLACE(job_json,'datax_reader_datasource_url','jdbc:db2://实际来源库IP:实际来源库端口/实际来源库名') where job_json LIKE '%datax_reader_datasource_url%' AND glue_type = 'DATAX';\r\n");
        bw.write("update job_info set job_json = REPLACE(job_json,'datax_reader_username','实际来源库加密后的用户名') where job_json LIKE '%datax_reader_username%' AND glue_type = 'DATAX';\r\n");
        bw.write("update job_info set job_json = REPLACE(job_json,'datax_reader_password','实际来源库加密后的密码') where job_json LIKE '%datax_reader_password%' AND glue_type = 'DATAX';\r\n");
        bw.write("*/\r\n");
        bw.write("--更新数据抽取任务目标库信息\r\n");
        bw.write("/*\r\n");
        bw.write("update job_info set job_json = REPLACE(job_json,'datax_writer_datasource_url','jdbc:db2://实际目标库IP:实际目标库端口/实际目标库名') where job_json LIKE '%datax_writer_datasource_url%' AND glue_type = 'DATAX';\r\n");
        bw.write("update job_info set job_json = REPLACE(job_json,'datax_writer_username','实际目标库加密后的用户名') where job_json LIKE '%datax_writer_username%' AND glue_type = 'DATAX';\r\n");
        bw.write("update job_info set job_json = REPLACE(job_json,'datax_writer_password','实际目标库加密后的密码') where job_json LIKE '%datax_writer_password%' AND glue_type = 'DATAX';\r\n");
        bw.write("*/");
        bw.flush();

        bw.close();
        fw.close();
        return file.getName();
    }

    /**
     * 拼接SQL语句，处理NULL
     *
     * @param sb
     * @param str
     * @return {@link StringBuffer}
     * @author jiangyang
     * @date 2020/11/12
     */
    private StringBuffer append(StringBuffer sb, String str) {
        if (str == null) {
            sb.append("NULL,");
        } else {
            sb.append("'").append(str.replace("'", "''")).append("',");
        }
        return sb;
    }
}
