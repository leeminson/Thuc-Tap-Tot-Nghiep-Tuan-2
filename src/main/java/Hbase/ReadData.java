/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author pc
 */
public class ReadData {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("test11");
            Table table = connection.getTable(tableName);
            Get get=new Get(Bytes.toBytes("r1"));
            Result result = table.get(get);
            byte[]personal=result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
            byte[]address=result.getValue(Bytes.toBytes("address"), Bytes.toBytes("city"));
            System.out.println("Personal info : "+Bytes.toString(personal));
            System.out.println("Address info : "+Bytes.toString(address));
        } catch (Exception e) {
        }
    }
}
