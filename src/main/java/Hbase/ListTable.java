/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 *
 * @author pc
 */
public class ListTable {
    public static void main(String[] args) {
        Configuration config=HBaseConfiguration.create();
        
        try {
            Connection con=ConnectionFactory.createConnection(config);
            Admin admin=con.getAdmin();
            HTableDescriptor[] hTableDescriptor=admin.listTables();
            for (HTableDescriptor hTableDescriptor1 : hTableDescriptor) {
                if(admin.isTableDisabled(hTableDescriptor1.getTableName())){
                    System.out.println(hTableDescriptor1.getNameAsString()+" is currently disabled");
                    System.out.println(hTableDescriptor1.getColumnFamilyCount());
                }
                
                else 
                {
                    System.out.println(hTableDescriptor1.getNameAsString()+" is active");
                    System.out.println(hTableDescriptor1.getColumnFamilyCount());
                }
            }
        } catch (IOException e) {
        }
        
    }
}
