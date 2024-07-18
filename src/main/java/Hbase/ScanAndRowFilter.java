/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Hbase;

/**
 *
 * @author pc
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class ScanAndRowFilter {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("test11"))) {
            Scan scan = new Scan();
            RowFilter rowFilter = new RowFilter(
                    CompareOp.EQUAL,
                    new RegexStringComparator("^r")
            );
            scan.setFilter(rowFilter);
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

