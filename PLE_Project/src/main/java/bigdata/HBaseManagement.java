package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

public class HBaseManagement {
	public static class HBasePictures extends Configured implements Tool {
		private static final byte[] FAMILY_IMAGE = Bytes.toBytes("image");
		private static final byte[][] COLUMN_IMAGE_PNG = { Bytes.toBytes("png") };

		private static final byte[] TABLE_NAME = Bytes.toBytes("famane1201_hgt");
		private static Table table = null;

		private enum Family {
			image(FAMILY_IMAGE, COLUMN_IMAGE_PNG);
			
			private byte[] name = Bytes.toBytes("");
			private byte[][] columns = Bytes.toByteArrays(Bytes.toBytes(""));
			
			private Family(byte[] name, byte[][] columns) {
				this.name = name;
				this.columns = columns;
			}
		}

		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}

		public static void createTable(Connection connection) {
			try {
				final Admin admin = connection.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				for (Family family: Family.values()) {
					tableDescriptor.addFamily(new HColumnDescriptor(family.name));
				}
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

        public static int storePictureData(byte[][] data) throws IOException {
			Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
			table = connection.getTable(TableName.valueOf(TABLE_NAME));
        	if (data == null || data.length < Family.values().length + 1) {
        		return -1;
        	}
    		int index = 0;
    		Put put = new Put(data[index++]);
    		for (Family family: Family.values()) {
    			for (int i = 0; i < family.columns.length; ++i) {
        			if (index == data.length) {
        				return -1;
        			}
    				put.addColumn(family.name, family.columns[i], data[index++]);
    			}
			}
    		table.put(put);
    		return 0;
    	}

		public int run(String[] args) throws IOException {
			Connection connection = ConnectionFactory.createConnection(getConf());
			createTable(connection);
			table = connection.getTable(TableName.valueOf(TABLE_NAME));
			return 0;
		}
	}
}
