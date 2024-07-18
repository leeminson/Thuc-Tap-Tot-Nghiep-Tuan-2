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
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author pc
 */
public class ScanAndPrefixFilter {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("test11"))) {
            Scan scan = new Scan();
            PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("a"));
            scan.setFilter(prefixFilter);
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    byte[] row = result.getRow();
                    System.out.println("Row: " + Bytes.toString(row));
                    for (Cell cell : result.rawCells()) {
                        System.out.println("Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                " Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                " Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
            }
        }
    }
}
