package ok.dht.test.shestakova;

import ok.dht.ServiceConfig;
import ok.dht.test.shestakova.dao.MemorySegmentDao;
import ok.dht.test.shestakova.exceptions.MethodNotAllowedException;
import ok.dht.test.shestakova.exceptions.NotEnoughReplicasException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Session;
import one.nio.net.Socket;
import one.nio.server.SelectorThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class DemoHttpServer extends HttpServer {

    public static final String INTERNAL_KEY = "internal";
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoHttpServer.class);
    private static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    private static final int NOT_FOUND_CODE = 404;
    public static final String INTERNAL_ERROR_IN_SERVER_MSG = "Internal error in server {}";
    public static final String ID = "id=";
    public static final String PATH = "/v0/entities";
    public static final String FROM = "from=";
    public static final String ACK = "ack=";
    public static final String START = "start=";
    public static final String END = "end=";
    public static final String SERVICE_MESSAGE_PATH = "/service/message";
    public static final String METHOD_NOT_ALLOWED_MSG = "Method not allowed {} method: {}";
    public static final String INTERNAL_ERROR_IN_SERVER_CF_MSG = "Internal error in server {} during working with CF";
    public static final String ERROR_WHILE_SENDING_RESPONSE_MSG = "Error while sending response in server {}";
    private final HttpClient httpClient;
    private final ServiceConfig serviceConfig;
    private final ExecutorService workersPool;
    private final CircuitBreakerImpl circuitBreaker;
    private final RequestsHandler requestsHandler;

    public DemoHttpServer(HttpServerConfig config, HttpClient httpClient, ExecutorService workersPool,
                          ServiceConfig serviceConfig, MemorySegmentDao dao, Object... routers) throws IOException {
        super(config, routers);
        this.httpClient = httpClient;
        this.serviceConfig = serviceConfig;
        this.workersPool = workersPool;
        this.circuitBreaker = new CircuitBreakerImpl(serviceConfig, httpClient);
        this.requestsHandler = new RequestsHandler(dao);
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        String requestPath = request.getPath();
        if (PATH.equals(requestPath)) {
            handleRangeRequest(request, session);
            return;
        }

        String key = request.getParameter(ID);
        if (key == null || key.isEmpty()) {
            tryToSendResponseWithEmptyBody(session, Response.BAD_REQUEST);
            return;
        }

        int methodNum = request.getMethod();
        if (methodNum != Request.METHOD_GET && methodNum != Request.METHOD_PUT && methodNum != Request.METHOD_DELETE) {
            tryToSendResponseWithEmptyBody(session, Response.METHOD_NOT_ALLOWED);
            return;
        }

        int from = getFrom(request.getParameter(FROM));
        int ack = getAck(request.getParameter(ACK), from);

        if (ack == 0 || ack > from || from > serviceConfig.clusterUrls().size()) {
            tryToSendResponseWithEmptyBody(session, Response.BAD_REQUEST);
            return;
        }

        executeRequestInWorkersPool(request, session, key, from, ack);
    }

    private void executeRequestInWorkersPool(Request request, HttpSession session, String key, int from, int ack) {
        workersPool.execute(() -> {
            try {
                executeHandlingRequest(request, session, key, ack, from);
            } catch (MethodNotAllowedException e) {
                LOGGER.error(METHOD_NOT_ALLOWED_MSG, serviceConfig.selfUrl(), request.getMethod());
                tryToSendResponseWithEmptyBody(session, Response.METHOD_NOT_ALLOWED);
            } catch (IOException e) {
                LOGGER.error(INTERNAL_ERROR_IN_SERVER_MSG, serviceConfig.selfUrl());
                tryToSendResponseWithEmptyBody(session, Response.INTERNAL_ERROR);
            }
        });
    }

    private static int getAck(String ackString, int from) {
        return ackString == null || ackString.isEmpty() ? from / 2 + 1 : Integer.parseInt(ackString);
    }

    private int getFrom(String fromString) {
        return fromString == null || fromString.isEmpty() ? serviceConfig.clusterUrls().size()
                : Integer.parseInt(fromString);
    }

    private void handleRangeRequest(Request request, HttpSession session) {
        String start = request.getParameter(START);
        String end = request.getParameter(END);
        if (start == null || start.isEmpty()) {
            tryToSendResponseWithEmptyBody(session, Response.BAD_REQUEST);
            return;
        }
        try {
            session.sendResponse(requestsHandler.handleGetRange(start, end));
        } catch (IOException e) {
            LOGGER.error(INTERNAL_ERROR_IN_SERVER_MSG, serviceConfig.selfUrl());
            tryToSendResponseWithEmptyBody(session, Response.INTERNAL_ERROR);
        }
    }

    @Override
    public HttpSession createSession(Socket socket) {
        return new DemoHttpSession(socket, this);
    }

    private void executeHandlingRequest(Request request, HttpSession session, String key, int ack, int from)
            throws IOException {
        if (request.getHeader(INTERNAL_KEY) != null || request.getPath().contains(SERVICE_MESSAGE_PATH)) {
            Response response = requestsHandler.handleInternalRequest(request, circuitBreaker);
            if (response == null) {
                tryToSendResponseWithEmptyBody(session, Response.SERVICE_UNAVAILABLE);
                return;
            }
            session.sendResponse(response);
            return;
        }

        List<String> targetNodes = HttpServerUtils.INSTANCE.getNodesSortedByRendezvousHashing(key, circuitBreaker,
                serviceConfig, from);
        List<HttpRequest> httpRequests = requestsHandler.getHttpRequests(request, key, targetNodes, serviceConfig);
        CompletableFuture<List<Response>> completableFuture = getResponses(request, ack, httpRequests)
                .whenComplete((list, throwable) -> {
                    if (throwable != null || list == null || list.size() < ack) {
                        tryToSendResponseWithEmptyBody(session, RESPONSE_NOT_ENOUGH_REPLICAS);
                        return;
                    }
                    try {
                        if (request.getMethod() != Request.METHOD_GET) {
                            session.sendResponse(list.get(0));
                            return;
                        }
                        aggregateResponsesAndSend(session, list);
                    } catch (IOException e) {
                        LOGGER.error(INTERNAL_ERROR_IN_SERVER_MSG, serviceConfig.selfUrl());
                        tryToSendResponseWithEmptyBody(session, Response.INTERNAL_ERROR);
                    }
                });
        checkCompletableFuture(completableFuture);
    }

    private void aggregateResponsesAndSend(HttpSession session, List<Response> responses) throws IOException {
        byte[] body = null;
        int notFoundResponsesCount = 0;
        long maxTimestamp = Long.MIN_VALUE;
        for (Response response : responses) {
            if (response.getStatus() == NOT_FOUND_CODE) {
                notFoundResponsesCount++;
                continue;
            }
            ByteBuffer bodyBB = ByteBuffer.wrap(response.getBody());
            long timestamp = bodyBB.getLong();
            if (maxTimestamp < timestamp) {
                maxTimestamp = timestamp;
                body = HttpServerUtils.INSTANCE.getBody(bodyBB);
            }
        }

        boolean cond = body != null && notFoundResponsesCount != responses.size();
        session.sendResponse(new Response(
                cond ? Response.OK : Response.NOT_FOUND,
                cond ? body : Response.EMPTY
        ));
    }

    private CompletableFuture<List<Response>> getResponses(Request request, int ack, List<HttpRequest> httpRequests) {
        final CompletableFuture<List<Response>> resultFuture = new CompletableFuture<>();
        List<Response> responses = new CopyOnWriteArrayList<>();
        AtomicInteger successCount = new AtomicInteger(ack);
        // если ack < from, допускаем from - ack + 1 ошибку
        AtomicInteger errorCount = new AtomicInteger(httpRequests.size() - ack + 1);
        for (HttpRequest httpRequest : httpRequests) {
            CompletableFuture<Response> completableFuture = (httpRequest == null ? getInternalResponse(request)
                    : proxyRequest(httpRequest))
                    .whenComplete((response, throwable) -> {
                        if (response == null) {
                            if (errorCount.decrementAndGet() == 0) {
                                resultFuture.completeExceptionally(new NotEnoughReplicasException());
                            }
                            return;
                        }
                        responses.add(response);
                        if (successCount.decrementAndGet() == 0) {
                            resultFuture.complete(responses);
                        }
                    });
            checkCompletableFuture(completableFuture);
        }
        return resultFuture;
    }

    private CompletableFuture<Response> getInternalResponse(Request request) {
        final CompletableFuture<Response> responseCompletableFuture = new CompletableFuture<>();
        workersPool.execute(() -> {
            Response response = requestsHandler.handleInternalRequest(request, circuitBreaker);
            if (response == null) {
                responseCompletableFuture.completeExceptionally(new IllegalArgumentException());
                return;
            }
            responseCompletableFuture.complete(response);
        });
        return responseCompletableFuture;
    }

    private CompletableFuture<Response> proxyRequest(HttpRequest httpRequest) {
        final CompletableFuture<Response> responseCompletableFuture = new CompletableFuture<>();
        CompletableFuture<HttpResponse<byte[]>> completableFuture = httpClient
                .sendAsync(
                        httpRequest,
                        HttpResponse.BodyHandlers.ofByteArray()
                )
                .whenComplete((response, throwable) -> {
                    if (throwable != null) {
                        responseCompletableFuture.completeExceptionally(new IllegalArgumentException());
                        return;
                    }
                    responseCompletableFuture.complete(new Response(
                            StatusCodes.statuses.getOrDefault(response.statusCode(), "UNKNOWN ERROR"),
                            response.body()
                    ));
                });
        checkCompletableFuture(completableFuture);
        return responseCompletableFuture;
    }

    private void checkCompletableFuture(CompletableFuture<?> completableFuture) {
        if (completableFuture == null) {
            LOGGER.error(DemoHttpServer.INTERNAL_ERROR_IN_SERVER_CF_MSG, serviceConfig.selfUrl());
        }
    }

    private void tryToSendResponseWithEmptyBody(HttpSession session, String resultCode) {
        try {
            session.sendResponse(new Response(
                    resultCode,
                    Response.EMPTY
            ));
        } catch (IOException e) {
            LOGGER.error(ERROR_WHILE_SENDING_RESPONSE_MSG, serviceConfig.selfUrl());
        }
    }

    @Override
    public synchronized void stop() {
        for (SelectorThread selectorThread : selectors) {
            if (!selectorThread.isAlive()) {
                continue;
            }
            for (Session session : selectorThread.selector) {
                session.close();
            }
        }
        circuitBreaker.doShutdownNow();
        super.stop();
    }
}
