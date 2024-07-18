package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.BasicConfigurator;

public class CreateTable {

    public static void main(String[] args) throws Exception {
        System.out.println("Creating Htable starts");

        Configuration config = HBaseConfiguration.create();
        try (
            Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("test11");
            if (!admin.tableExists(tableName)) {
                HTableDescriptor htable = new HTableDescriptor(tableName);
                htable.addFamily(new HColumnDescriptor("personal"));
                htable.addFamily(new HColumnDescriptor("address"));
                admin.createTable(htable);
            } else {
                System.out.println("test Htable is exists");
            }
            admin.close();
        }
        System.out.println("Creating Htable Done");
    }
}
