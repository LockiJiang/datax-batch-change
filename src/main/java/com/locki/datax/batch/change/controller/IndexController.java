package com.locki.datax.batch.change.controller;

import com.locki.datax.batch.change.service.JobInfoService;
import com.locki.datax.batch.change.vo.JobsSearchVo;
import com.locki.datax.batch.change.vo.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;


/**
 * @author jiangyang
 * @date 2020/11/10
 */
@RestController
public class IndexController {
    @Autowired
    private JobInfoService jobInfoService;

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
        List<Map<String, String>> list = null;
        try {
            list = jobInfoService.listJobs(vo);
        } catch (SQLException e) {
            e.printStackTrace();
            return Result.fail(500, "获取任务异常！", e.getMessage());
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
        List<Map<String, String>> list = jobInfoService.getData(vo);
        if (list == null || list.size() < 1) {
            return "未找到有效的任务信息！";
        }
        jobInfoService.addTrunc(vo, list);
        return "处理完成";
    }

    /**
     * 为所选任务添加pre/post语句
     *
     * @param vo
     * @return {@link String}
     * @author jiangyang
     * @date 2022/2/14
     */
    @PostMapping("/addSql")
    public String addSql(@RequestBody @Valid JobsSearchVo vo) {
        if (vo == null || vo.getSql() == null || vo.getSql().trim().length() < 1) {
            return "未设置有效的SQL！";
        }

        if (vo == null || vo.getType() == null
                || !(vo.getType().trim().equals("post") || vo.getType().trim().equals("pre"))) {
            return "请指定pre或者post！";
        }

        List<Map<String, String>> list = jobInfoService.getData(vo);
        if (list == null || list.size() < 1) {
            return "未找到有效的任务信息！";
        }
        jobInfoService.addSql(vo, list);
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
        List<Map<String, String>> list = jobInfoService.getData(vo);
        //处理任务json脚本
        list = jobInfoService.changeDatasource(list);
        //生成脚本文件
        try {
            res = jobInfoService.exportFile(list);
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
}
