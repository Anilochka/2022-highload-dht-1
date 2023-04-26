package ok.dht.test.shestakova;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import ok.dht.ServiceConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class HttpServerUtilsTest {
    private static final int SELF_PORT = 1111;
    private static final String SELF_URL = "";
    private static final String PATH = "./";
    private static final String KEY = "key";
    private static final String BODY = "body";
    private final CircuitBreakerImpl circuitBreaker = Mockito.mock(CircuitBreakerImpl.class);
    private final List<String> clusterUrls = new ArrayList<>();
    private ServiceConfig serviceConfig = new ServiceConfig(SELF_PORT, SELF_URL, clusterUrls, Paths.get(PATH));
    @Test
    void getNodesSortedByRendezvousHashing() {
        int from = 10;

        Assertions.assertEquals(0, HttpServerUtils.INSTANCE.getNodesSortedByRendezvousHashing(
                        KEY, circuitBreaker, serviceConfig, from).size()
        );

        clusterUrls.add(SELF_URL);
        serviceConfig = new ServiceConfig(SELF_PORT, SELF_URL, clusterUrls, Paths.get(PATH));
        assertEquals(1, HttpServerUtils.INSTANCE.getNodesSortedByRendezvousHashing(
                KEY, circuitBreaker, serviceConfig, from).size()
        );
        assertEquals(clusterUrls.get(0), HttpServerUtils.INSTANCE.getNodesSortedByRendezvousHashing(
                KEY, circuitBreaker, serviceConfig, from).get(0));

        from = 0;
        assertEquals(0, HttpServerUtils.INSTANCE.getNodesSortedByRendezvousHashing(
                KEY, circuitBreaker, serviceConfig, from).size());
    }

    @Test
    void getBody() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        byteBuffer.position(Long.BYTES);
        byteBuffer.putInt(-1);

        assertNull(HttpServerUtils.INSTANCE.getBody(byteBuffer));

        byte[] body = BODY.getBytes();
        int bodySize = body.length;
        byteBuffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + bodySize);
        byteBuffer.position(Long.BYTES);
        byteBuffer.putInt(bodySize);
        byteBuffer.put(body);

        assertArrayEquals(body, HttpServerUtils.INSTANCE.getBody(byteBuffer));
    }

    @Test
    void fromString() {
        assertNull(HttpServerUtils.INSTANCE.fromString(null));
    }
}
