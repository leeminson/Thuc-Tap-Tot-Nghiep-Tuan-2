/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author pc
 */
public class ScanAndSingleColumnValueFilter {
     public static void main(String[] args) throws IOException  {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("test11"))) {

            // Tạo đối tượng Scan
            Scan scan = new Scan();

            // Thêm filter để chỉ quét những hàng có giá trị cột cf:qualifier1 là "some_value"
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("personal"),
                    Bytes.toBytes("name"),
                    CompareOperator.EQUAL,
                    Bytes.toBytes("Son")
            );

            // Chỉ trả về các hàng có cột thỏa mãn filter
            filter.setFilterIfMissing(true);

            // Đặt filter cho Scan
            scan.setFilter(filter);

            // Thực hiện Scan
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    // Xử lý kết quả
                    byte[] row = result.getRow();
                    byte[] value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
                    System.out.println("Row: " + Bytes.toString(row) + ", Value: " + Bytes.toString(value));
                }
            }
        }
    }
}

