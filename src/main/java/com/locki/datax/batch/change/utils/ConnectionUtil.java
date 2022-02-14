package com.locki.datax.batch.change.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 数据库连接
 *
 * @author jiangyang
 * @date 2020/11/11
 */
public class ConnectionUtil {

    /**
     * 获取数据库连接
     *
     * @param dbHost:数据库host
     * @param dbPort:数据库端口
     * @param dbName:数据库名
     * @param dbUsername:数据库用户
     * @param dbPassword:数据库密码
     * @return
     */
    public static Connection getCon(String dbHost, int dbPort, String dbName, String dbUsername, String dbPassword) {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dbUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName + "?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true";
            con = (Connection) DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

    /**
     * 关闭资源
     *
     * @param con
     * @param st
     * @param rs
     */
    public static void close(Connection con, Statement st, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (Exception e2) {
            e2.printStackTrace();
        }
    }
}
