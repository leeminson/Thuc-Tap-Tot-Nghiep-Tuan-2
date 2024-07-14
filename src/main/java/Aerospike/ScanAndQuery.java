import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Query;
import com.aerospike.client.query.Scan;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.ClientPolicy;

public class ScanAndQuery {
    public static void main(String[] args) {
        // Configure Aerospike client policy with NIO event loops
        ClientPolicy policy = new ClientPolicy();
        policy.eventLoops = new NioEventLoops();

        // Connect to Aerospike cluster
        AerospikeClient client = new AerospikeClient(policy, "localhost", 3000);

        try {
            // Write records
            Key key1 = new Key("test", "demo", "key1");
            Bin bin1 = new Bin("bin1", "value1");
            client.put(null, key1, bin1);

            Key key2 = new Key("test", "demo", "key2");
            Bin bin2 = new Bin("bin1", "value2");
            client.put(null, key2, bin2);

            // Create secondary index on bin1
            client.createIndex(null, "test", "demo", "bin1_index", "bin1", IndexType.STRING);

            // Perform Query
            Statement stmt = new Statement();
            stmt.setNamespace("test");
            stmt.setSetName("demo");
            stmt.setBinNames("bin1");
            stmt.setFilter(Filter.equal("bin1", "value1"));

            client.q(null, stmt, (key, record) -> {
                System.out.println("Query Record Key: " + key);
                System.out.println("Query Record Value: " + record.getValue("bin1"));
            });

            // Perform Scan
            ScanPolicy scanPolicy = new ScanPolicy();
            Scan scan = new Scan("test", "demo");
            client.scanAll(null, scan, (key, record) -> {
                System.out.println("Scan Record Key: " + key);
                System.out.println("Scan Record Value: " + record);
            });
        } finally {
            // Close Aerospike client
            client.close();
        }
    }
}
