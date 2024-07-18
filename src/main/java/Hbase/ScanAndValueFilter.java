package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;

public class ScanAndValueFilter {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("test11"))) {

            // Tạo đối tượng Scan
            Scan scan = new Scan();

            // Thêm ValueFilter để chỉ quét những ô có giá trị bằng "some_value"
            ValueFilter valueFilter = new ValueFilter(
                    CompareOperator.EQUAL,
                    new BinaryComparator(Bytes.toBytes("Son"))
            );

            // Đặt filter cho Scan
            scan.setFilter(valueFilter);

            // Thực hiện Scan
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
