package ok.dht.test.shestakova;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jdk.incubator.foreign.MemorySegment;
import ok.dht.ServiceConfig;
import one.nio.util.Hash;

public class HttpServerUtils {

    static final HttpServerUtils INSTANCE = new HttpServerUtils();
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerUtils.class);

    List<String> getNodesSortedByRendezvousHashing(String key, CircuitBreakerImpl circuitBreaker,
                                                   ServiceConfig serviceConfig, int from) {
        Map<Integer, String> nodesHashes = new TreeMap<>();

        for (String nodeUrl : serviceConfig.clusterUrls()) {
            if (circuitBreaker.isNodeIll(nodeUrl)) {
                LOGGER.info("Node is ill: {}", nodeUrl);
                continue;
            }
            nodesHashes.put(Hash.murmur3(nodeUrl + key), nodeUrl);
        }
        LOGGER.info("Sorted nodes by rendezvous hashing, init count = {}, result count = {}",
                serviceConfig.clusterUrls().size(), nodesHashes.size());
        return nodesHashes.values().stream()
                .limit(from)
                .toList();
    }

    byte[] getBody(ByteBuffer bodyBB) {
        byte[] body;
        bodyBB.position(Long.BYTES);
        int valueLength = bodyBB.getInt();
        if (valueLength == -1) {
            body = null;
            LOGGER.debug("Handle empty requests body");
        } else {
            body = new byte[valueLength];
            bodyBB.get(body, 0, valueLength);
        }
        return body;
    }

    MemorySegment fromString(String data) {
        if (data == null) {
            LOGGER.debug("Creating MemorySegment of NULL string");
        }
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }
}
