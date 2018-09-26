package com.sijifeng.example.impala;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
/**
 * Created by yingchun on 2018/9/21.
 */
public class ImpalaJdbcClient {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    //默认数据库default,默认用户名就是登录账号 密码 为空,impala server dn3.hadoop
    private static String CONNECTION_URL = "jdbc:impala://192.168.0.22:21050/default;auth=noSasl";//nn1.hadoop,database:default
    private static final Logger log = Logger.getLogger(ImpalaJdbcClient.class);
    public static void main(String[] args) {
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps = null;

        try
        {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(CONNECTION_URL);
            ps = con.prepareStatement("select * from mapping_java_sample_1537430886694  limit 2;");
            rs = ps.executeQuery();
            while (rs.next())
            {
                System.out.println(rs.getString(1) + '\t' + rs.getString(2));
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            //关闭rs、ps和con
        }
    }

    //get db connection
    public static Statement getDBconnection()
    {
        try {
            Class.forName(JDBC_DRIVER);
            Connection conn = DriverManager.getConnection(CONNECTION_URL);
            return conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error(JDBC_DRIVER + " not found!", e);
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        }
        return null;
    }
}
