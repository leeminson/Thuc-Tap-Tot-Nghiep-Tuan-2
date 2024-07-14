/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 *
 * @author pc
 */
public class AerospikeNetty {

    public static void main(String[] args) {
        EventPolicy eventPolicy = new EventPolicy();
        EventLoopGroup group = new NioEventLoopGroup(4);
        EventLoops eventLoops = new NettyEventLoops(eventPolicy, group);
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.eventLoops = eventLoops;
        AerospikeClient client = new AerospikeClient(clientPolicy, "127.0.0.1", 3000);
        WritePolicy writePolicy = new WritePolicy();
        Key key = new Key("test", "demo", "key1");
        Bin bin = new Bin("name", "John Doe");
        client.put(writePolicy, key, bin);
        client.close();
        eventLoops.close();
        group.shutdownGracefully();
    }
}
