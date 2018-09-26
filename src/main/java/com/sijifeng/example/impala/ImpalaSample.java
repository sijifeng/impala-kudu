package com.sijifeng.example.impala;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
/**
 * Created by yingchun on 2018/9/21.
 */
public class ImpalaSample {
    public static void main (String[] args) {

        String sqlStatement = "select * from mapping_java_sample_1537430886694 limit 10";


        Connection con = null;

        try {

            Class.forName("com.cloudera.impala.jdbc41.Driver");

            con = DriverManager.getConnection("jdbc:impala://192.168.0.22:21050");

            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            System.out.println("\n== Begin Query Results ======================");

            // print the results to the console
            while (rs.next()) {
                // the example query returns one String column
                System.out.println(rs.getString(1));
            }

            System.out.println("== End Query Results =======================\n\n");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (Exception e) {
                // swallow
            }
        }

    }
}
