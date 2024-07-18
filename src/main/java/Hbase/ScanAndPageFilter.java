/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author pc
 */
public class ScanAndPageFilter {
     public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("test11"))) {
            int pageSize = 2;
            byte[] lastRow = null;
            for (int pageIndex = 0; pageIndex < 5; pageIndex++) {
                Scan scan = new Scan();
                scan.setFilter(new PageFilter(pageSize));
                if (lastRow != null) {
                    scan.withStartRow(lastRow, false);
                }
                try (ResultScanner scanner = table.getScanner(scan)) {
                    int rowCount = 0;
                    for (Result result : scanner) {
                        byte[] row = result.getRow();
                        System.out.println("Page: " + pageIndex + " Row: " + Bytes.toString(row));
                        for (Cell cell : result.rawCells()) {
                            System.out.println("Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                    " Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                    " Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                        }
                        lastRow = row;
                        rowCount++;
                    }
                    if (rowCount < pageSize) {
                        break; 
                    }
                }
            }
        }
    }
}
