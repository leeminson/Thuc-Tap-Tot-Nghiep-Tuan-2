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
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author pc
 */
public class TTL {
    public static void main(String[] args) throws IOException {
        // Cấu hình HBase
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();

            // Tên bảng
            TableName tableName = TableName.valueOf("test111");

            // Kiểm tra tồn tại của bảng
            if (!admin.tableExists(tableName)) {
                // Định nghĩa họ cột với TTL (3600 giây = 1 giờ)
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
                        .setTimeToLive(60) // TTL tính bằng giây
                        .build();

                // Định nghĩa bảng với họ cột
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(columnFamilyDescriptor)
                        .build();

                // Tạo bảng
                admin.createTable(tableDescriptor);
                System.out.println("Table '" + tableName + "' created successfully with TTL.");
            } else {
                System.out.println("Table '" + tableName + "' already exists.");
            }
        }
    }
}
