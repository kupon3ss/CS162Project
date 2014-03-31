package kvstore;

import java.net.InetAddress;

public class SampleClient {

    public static void main(String[] args) {
        try {
            String hostname = InetAddress.getLocalHost().getHostAddress();
            KVClient client = new KVClient(hostname, 8080);

            System.out.println("put(\"foo\", \"bar\")");
            client.put("foo", "bar");
            System.out.println("put success!");

            System.out.println("get(\"foo\")");
            String value = client.get("foo");
            System.out.println("returned \"" + value + "\"");

            System.out.println("del(\"foo\")");
            client.del("foo");
            System.out.println("del success!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
