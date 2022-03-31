import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDemo {
    public static void main(String[] args) throws IOException {

        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        System.out.println("HBase is running!");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("yagamishi:student");
        String colFamily1 = "info";
        String colFamily2 = "score";

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Delete First");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(colFamily1);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
            hTableDescriptor.addFamily(hColumnDescriptor1);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            admin.createTable(hTableDescriptor);
            System.out.println("create successful");
        }

        // 插入数据
        String rowKey = "yagamishi";
        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("G20210616020010")); // col1
        put.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes(String.valueOf(1))); // col2
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(String.valueOf(99))); // col1
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programing"), Bytes.toBytes(String.valueOf(99))); // col2
        conn.getTable(tableName).put(put);
        System.out.println("insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}