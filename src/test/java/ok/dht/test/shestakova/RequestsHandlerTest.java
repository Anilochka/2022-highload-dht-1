package ok.dht.test.shestakova;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ok.dht.ServiceConfig;
import ok.dht.test.shestakova.dao.MemorySegmentDao;
import ok.dht.test.shestakova.dao.base.BaseEntry;
import ok.dht.test.shestakova.dao.base.Config;
import one.nio.http.Request;
import one.nio.http.Response;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RequestsHandlerTest {
    public static final String PATH = "./";
    public static final int FLUSH_THRESHOLD_BYTES = 1024;
    public static final String BODY = "body";
    public static final int SELF_PORT = 1111;
    public static final String SELF_URL = "";
    public static final String KEY = "key";
    public static final String ID = "1";
    private final int methodGet = 1;
    private final String uri = "";
    private final Request request = new Request(methodGet, uri, false);
    private final Config config = new Config(Paths.get(PATH), FLUSH_THRESHOLD_BYTES);
    private MemorySegmentDao dao;
    private RequestsHandler requestsHandler;

    RequestsHandlerTest() {
        request.setBody(BODY.getBytes());
    }

    @BeforeEach
    void setUp() throws IOException {
        dao = new MemorySegmentDao(config);
        requestsHandler = new RequestsHandler(dao);
    }

    @Test
    void getHttpRequests() {
        List<String> clusterUrls = new ArrayList<>();
        clusterUrls.add(uri);
        Path workingDir = Paths.get(PATH);
        ServiceConfig serviceConfig = new ServiceConfig(SELF_PORT, SELF_URL, clusterUrls, workingDir);
        List<HttpRequest> httpRequests = requestsHandler.getHttpRequests(request, KEY, clusterUrls, serviceConfig);
        assertEquals(1, httpRequests.size());
        assertNull(httpRequests.get(0));

        clusterUrls.add("http://host");
        serviceConfig = new ServiceConfig(SELF_PORT, SELF_URL, clusterUrls, workingDir);
        httpRequests = requestsHandler.getHttpRequests(request, KEY, clusterUrls, serviceConfig);

        assertEquals(2, httpRequests.size());
        int notNullCount = 0;
        for (HttpRequest httpRequest : httpRequests) {
            if (httpRequest != null) {
                notNullCount++;
            }
        }

        assertEquals(1, notNullCount);
    }

    @Test
    void handleGet() {
        assertEquals(Response.NOT_FOUND, requestsHandler.handleGet(null).getHeaders()[0]);
        assertEquals(Response.NOT_FOUND, requestsHandler.handleGet(KEY).getHeaders()[0]);

        dao.upsert(new BaseEntry<>(HttpServerUtils.INSTANCE.fromString(ID), HttpServerUtils.INSTANCE.fromString(ID), 0L));
        assertEquals(Response.OK, requestsHandler.handleGet(ID).getHeaders()[0]);

        dao.upsert(new BaseEntry<>(HttpServerUtils.INSTANCE.fromString(ID), null, 0L));
        assertEquals(Response.OK, requestsHandler.handleGet(ID).getHeaders()[0]);
    }

    @Test
    void handleGetRange() {
        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(ID, null).getHeaders()[0]),
                () -> assertEquals(Response.EMPTY, requestsHandler.handleGetRange(ID, null).getBody())
        );

        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(ID, ID).getHeaders()[0]),
                () -> assertEquals(Response.EMPTY, requestsHandler.handleGetRange(ID, ID).getBody())
        );

        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(null, ID).getHeaders()[0]),
                () -> assertEquals(Response.EMPTY, requestsHandler.handleGetRange(null, ID).getBody())
        );

        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(null, null).getHeaders()[0]),
                () -> assertEquals(Response.EMPTY, requestsHandler.handleGetRange(null, null).getBody())
        );

        dao.upsert(new BaseEntry<>(HttpServerUtils.INSTANCE.fromString(ID), HttpServerUtils.INSTANCE.fromString(KEY), 0L));

        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(ID, null).getHeaders()[0]),
                () -> assertNotEquals(Response.EMPTY, requestsHandler.handleGetRange(ID, null).getBody())
        );
        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(ID, ID).getHeaders()[0]),
                () -> assertEquals(Response.EMPTY, requestsHandler.handleGetRange(ID, ID).getBody())
        );
        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(null, ID).getHeaders()[0]),
                () -> assertEquals(Response.EMPTY, requestsHandler.handleGetRange(null, ID).getBody())
        );
        assertAll(
                () -> assertEquals(Response.OK, requestsHandler.handleGetRange(null, null).getHeaders()[0]),
                () -> assertNotEquals(Response.EMPTY, requestsHandler.handleGetRange(null, null).getBody())
        );
    }

    @Test
    void handlePut() {
        assertEquals(Response.NOT_FOUND, requestsHandler.handlePut(request, null).getHeaders()[0]);
        assertEquals(Response.CREATED, requestsHandler.handlePut(request, ID).getHeaders()[0]);
    }

    @Test
    void handleDelete() {
        assertEquals(Response.NOT_FOUND, requestsHandler.handleDelete(null).getHeaders()[0]);
        assertEquals(Response.ACCEPTED, requestsHandler.handleDelete(ID).getHeaders()[0]);
    }

    @AfterEach
    public void setDown() throws IOException {
        dao.close();
    }
}
