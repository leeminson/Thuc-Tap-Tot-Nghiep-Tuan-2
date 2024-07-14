package Aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;

public class AerospikeNio {

    public static void main(String[] args) {

        ClientPolicy policy = new ClientPolicy();
        policy.eventLoops = new NioEventLoops();

        AerospikeClient client = new AerospikeClient(policy, "localhost", 3000);

        try {
            Key key = new Key("test", "demo", "key1");
            Bin bin1 = new Bin("name", "value2");
            client.put(null, key, bin1);

            System.out.println("Record written successfully.");
            Record record = client.get(null, key);
            if (record != null) {
                System.out.println("Record found: " + record.getValue("bin1"));
            } else {
                System.out.println("Record not found.");
            }
        } finally {
            // Close Aerospike client
            client.close();
        }
    }
}
