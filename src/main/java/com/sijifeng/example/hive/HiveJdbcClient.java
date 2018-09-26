package com.sijifeng.example.hive;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;


import org.apache.log4j.Logger;
/**
 * Created by yingchun on 2018/9/21.
 */
public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    //默认数据库default,默认用户名就是登录账号 密码 为空...
    private static String url = "jdbc:hive2://192.168.0.22:10000/default";//nn1.hadoop,database:default
    private static final Logger log = Logger.getLogger(HiveJdbcClient.class);
    public static void main(String[] args) {
        try {
            Statement stmt = getDBconnection();
            String sql = "select * from sfmta_raw limit 10";
            System.out.println("Running sql: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
            }
        }catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        }
    }


    //get db connection
    public static Statement getDBconnection()
    {
        try {
            Class.forName(driverName);
            Connection conn = DriverManager.getConnection(url);
            return conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error(driverName + " not found!", e);
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        }
        return null;
    }

}
