package bigdata;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseManagement {
	public static class HBaseProg extends Configured implements Tool {
        private static final byte[] FAMILY = Bytes.toBytes("preview");
        private static final byte[] FAMILY_IMG= Bytes.toBytes("image");
        private static final byte[] FAMILY_LOC = Bytes.toBytes("location");
      //  private static final byte[] ROW    = Bytes.toBytes("");
        private static final byte[] TABLE_NAME = Bytes.toBytes("fmn");
        private static Table table = null;
        
        public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
        }

        public static void createTable(Connection connect) {
            try {
                final Admin admin = connect.getAdmin(); 
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                HColumnDescriptor famMap = new HColumnDescriptor(FAMILY); 
                //famLoc.set...
                tableDescriptor.addFamily(famMap);
                createOrOverwrite(admin, tableDescriptor);
                admin.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        
        public static void storeData(String rowName, int locX, int locY, byte[] img) throws IOException {
    		Put p = new Put(Bytes.toBytes(rowName));
        	p.addColumn(FAMILY,FAMILY_LOC, Bytes.toBytes(locX));
    		p.addColumn(FAMILY,FAMILY_LOC, Bytes.toBytes(locY));
    		p.addColumn(FAMILY,FAMILY_IMG, img);   
    		table.put(p);
    	}
        
        

        public int run(String[] args) throws IOException {
            Connection connection = ConnectionFactory.createConnection(getConf());
            createTable(connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            return 0;
        }

    }
}
