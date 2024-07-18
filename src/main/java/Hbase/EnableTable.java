/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 *
 * @author pc
 */
public class EnableTable {

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try {

            Connection con = ConnectionFactory.createConnection(config);
            Admin admin = con.getAdmin();
            TableName test11 = TableName.valueOf("test11");
            Boolean isActive = admin.isTableEnabled(test11);
            System.out.println("Table is Enabled : " + isActive);
            if (!isActive) {
                admin.enableTable(test11);
                System.out.println(test11.getNameAsString() + " is Enabled");
            }

        } catch (IOException e) {
        }
    }
}
