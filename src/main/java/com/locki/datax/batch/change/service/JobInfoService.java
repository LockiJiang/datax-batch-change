package com.locki.datax.batch.change.service;

import com.locki.datax.batch.change.dao.JobInfoDao;
import com.locki.datax.batch.change.utils.Configuration;
import com.locki.datax.batch.change.vo.JobsSearchVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * 任务处理逻辑
 *
 * @author jiangyang
 * @date 2022/2/14
 */
@Service
public class JobInfoService {
    @Autowired
    private JobInfoDao jobInfoDao;

    /**
     * 获取datax任务
     *
     * @param List<Map<String, String>>
     * @return {@link null}
     * @author jiangyang
     * @date 2022/2/14
     */
    public List<Map<String, String>> listJobs(JobsSearchVo vo) throws SQLException {
        return jobInfoDao.listJobs(vo);
    }

    /**
     * 查询任务信息
     *
     * @param vo
     * @return {@link List<Map<String, String>>}
     * @author jiangyang
     * @date 2020/11/12
     */
    public List<Map<String, String>> getData(JobsSearchVo vo) {
        if (vo.getIds() == null || vo.getIds().size() < 1) {
            return new ArrayList<>();
        }
        return jobInfoDao.getData(vo);
    }

    /**
     * 为所选任务添加truncate语句
     *
     * @param vo
     * @param list
     * @return {@link String}
     * @author jiangyang
     * @date 2022/2/14
     */
    public String addTrunc(JobsSearchVo vo, List<Map<String, String>> list) {
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
        jobInfoDao.updateJobJson(vo, list);
        return "处理完成";
    }

    /**
     * 为所选任务添加pre/post语句
     *
     * @param vo
     * @param list
     * @return {@link String}
     * @author jiangyang
     * @date 2022/2/14
     */
    public String addSql(JobsSearchVo vo, List<Map<String, String>> list) {
        for (Map<String, String> map : list) {
            Configuration conf = Configuration.from(map.get("job_json"));
            //获取已有的pre或者post
            List<String> sqls;
            if ("pre".equals(vo.getType().trim())) {
                sqls = conf.getList("job.content[0].writer.parameter.preSql", String.class);
            } else {
                sqls = conf.getList("job.content[0].writer.parameter.postSql", String.class);
            }
            if (sqls == null) {
                sqls = new ArrayList<>();
            }
            boolean haveSql = false;
            Iterator<String> it = sqls.iterator();
            while (it.hasNext()) {
                String sql = it.next();
                if (sql == null || sql.trim().length() < 1) {
                    it.remove();
                    continue;
                }
                if (sql.trim().equals(vo.getSql().trim())) {
                    haveSql = true;
                    break;
                }
            }
            if (!haveSql) {
                sqls.add(vo.getSql().trim());
            }
            //更新
            if ("pre".equals(vo.getType().trim())) {
                conf.set("job.content[0].writer.parameter.preSql", sqls);
            } else {
                conf.set("job.content[0].writer.parameter.postSql", sqls);
            }
            map.put("job_json", conf.toString());
            System.out.println(conf);
        }
        //jobInfoDao.updateJobJson(vo, list);
        return "处理完成";
    }

    /**
     * 处理datax任务json基本，将reader和writer的数据连接信息改为特殊信息，方便replace
     *
     * @param list
     * @return
     * @author jiangyang
     * @date 2020/11/11
     */
    public List<Map<String, String>> changeDatasource(List<Map<String, String>> list) {
        if (list == null || list.size() < 1) {
            return list;
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
        return list;
    }

    /**
     * 生成SQL脚本文件
     *
     * @param list
     * @return
     * @author jiangyang
     * @date 2020/11/11
     */
    public String exportFile(List<Map<String, String>> list) throws IOException {
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
