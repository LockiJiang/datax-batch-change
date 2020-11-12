package com.locki.datax.batch.change.vo;

import lombok.Data;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.util.List;


/**
 * 数据源连接参数
 *
 * @author jiangyang
 * @date 2020/11/10
 */
@Data
public class JobsSearchVo {
    @NotBlank(message = "数据源地址不能为空")
    private String dbHost;
    @NotNull(message = "数据源端口不能为空")
    private Integer dbPort;
    @NotBlank(message = "数据源库名不能为空")
    private String dbName;
    @NotBlank(message = "数据源用户名不能为空")
    private String dbUsername;
    @NotBlank(message = "数据源密码不能为空")
    private String dbPassword;
    private String jobDesc;
    private String projectName;
    private List<Integer> ids;
}
