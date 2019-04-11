package client;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class Client {
    // A grid client instance
    protected Ignite ignite;

    // Required to defreeze a transaction that is in the deadlock and trigger the deadlock detection.
    protected static int TX_TIMEOUT = 3000;

    public Client(String igniteConfig) {
        ignite = Ignition.start(igniteConfig);
    }

    public void stop() {
        Ignition.stop(true);
    }
}
