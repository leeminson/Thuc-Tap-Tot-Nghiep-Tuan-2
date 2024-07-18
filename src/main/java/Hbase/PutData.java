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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author pc
 */
public class PutData {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            
            TableName tableName = TableName.valueOf("test11");
            Table table = connection.getTable(tableName);
            Put p=new Put(Bytes.toBytes("r4"));
//            p.addColumn(Bytes.toBytes("personal"),Bytes.toBytes("name"),Bytes.toBytes("Son"));
            p.addColumn(Bytes.toBytes("address"),Bytes.toBytes("city"),Bytes.toBytes("Lang Son"));        
            table.put(p);
        } catch (Exception e) {
        }
    }
}
