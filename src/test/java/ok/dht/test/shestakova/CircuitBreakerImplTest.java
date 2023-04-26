package ok.dht.test.shestakova;

import java.net.http.HttpClient;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ok.dht.ServiceConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class CircuitBreakerImplTest {
    private static final List<String> clusterUrls = new ArrayList<>();
    private static CircuitBreakerImpl circuitBreaker;
    private static final String PATH = "./";
    private static final String NODE_URL = "1";
    private static final int SELF_PORT = 1111;
    private static final String SELF_URL = "";

    @BeforeAll
    public static void setUp() {
        clusterUrls.add(NODE_URL);
        ServiceConfig serviceConfig = new ServiceConfig(SELF_PORT, SELF_URL, clusterUrls, Paths.get(PATH));
        circuitBreaker = new CircuitBreakerImpl(serviceConfig, Mockito.mock(HttpClient.class));
    }

    @Test
    void putNodesIllnessInfo() {
        assertFalse(circuitBreaker.isNodeIll(NODE_URL));
        circuitBreaker.putNodesIllnessInfo(NODE_URL, true);
        assertTrue(circuitBreaker.isNodeIll(NODE_URL));

        circuitBreaker.putNodesIllnessInfo(NODE_URL, false);
        assertFalse(circuitBreaker.isNodeIll(NODE_URL));

        int exceptionsCount = 0;
        try {
            circuitBreaker.putNodesIllnessInfo(SELF_URL, true);
        } catch (Exception e) {
            exceptionsCount++;
        }
        assertEquals(1, exceptionsCount);
        try {
            circuitBreaker.putNodesIllnessInfo(SELF_URL, false);
        } catch (Exception e) {
            exceptionsCount++;
        }
        assertEquals(2, exceptionsCount);
        try {
            circuitBreaker.putNodesIllnessInfo(null, true);
        } catch (Exception e) {
            exceptionsCount++;
        }
        assertEquals(3, exceptionsCount);
        try {
            circuitBreaker.putNodesIllnessInfo(null, false);
        } catch (Exception e) {
            exceptionsCount++;
        }
        assertEquals(4, exceptionsCount);
    }

    @Test
    void isNodeIll() {
        int exceptionsCount = 0;
        try {
            circuitBreaker.isNodeIll(SELF_URL);
        } catch (Exception e) {
            exceptionsCount++;
        }
        assertEquals(1, exceptionsCount);
        try {
            circuitBreaker.isNodeIll(null);
        } catch (Exception e) {
            exceptionsCount++;
        }
        assertEquals(2, exceptionsCount);
        assertFalse(circuitBreaker.isNodeIll(NODE_URL));
    }

    @AfterAll
    public static void setDown() {
        circuitBreaker.doShutdownNow();
    }
}
