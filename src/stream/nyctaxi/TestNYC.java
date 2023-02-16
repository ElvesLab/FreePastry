package stream.nyctaxi;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;

import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastImpl;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;
import rice.persistence.PersistentStorage;
import rice.persistence.Storage;
import rice.persistence.StorageManagerImpl;
import rice.tutorial.past.MyPastContent;
import stream.etl.CSVReader;

public class TestNYC {

    public static void main(String[] args) throws IOException {
        String filePath = "/home/parallels/Desktop/taxi_csvs/test.csv";
        CSVReader reader = new CSVReader()
                        .filePath(filePath)
                        .rowKeyIdx(0)
                        .versionIdx(3, true)
                        .buildReader();
        

        if (reader == null) {
            System.err.println("Error in intilizaling reader");
        }

        InetAddress bootaddr = InetAddress.getByName("10.211.55.3");
        int bootport = 9006;
        InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);

        Environment env = new Environment();
        PastryIdFactory localFactory = new rice.pastry.commonapi.PastryIdFactory(env);
        env.getParameters().setString("nat_search_policy","never");
        NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
        PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bootport, env);
        PastryNode node = factory.newNode();
        PastryIdFactory idf = new rice.pastry.commonapi.PastryIdFactory(env);
        String storageDirectory = "./storage"+node.getId().hashCode();

        // create the persistent part
        Storage stor = new PersistentStorage(idf, storageDirectory, 4 * 1024 * 1024, node
            .getEnvironment());
        Past app = new PastImpl(node, new StorageManagerImpl(idf, stor, new LRUCache(
        new MemoryStorage(idf), 512 * 1024, node.getEnvironment())), 0, "");

        node.boot(bootaddress);

        HashMap<String, String> data;
        try {
            data = reader.readOneLine();
            while (data != null) {
                System.out.println(data);

                PastContent pContent = new MyPastContent(localFactory.buildId(data.get("other")), data.get("other"));
                app.insert(pContent, new Continuation<Boolean[], Exception>() {
                    public void receiveResult(Boolean[] results) {
                        System.out.println("rly");
                    }

                    public void receiveException(Exception result) {
                    }
                });
                
                // break;
                // get node in pastry, and insert the data
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
