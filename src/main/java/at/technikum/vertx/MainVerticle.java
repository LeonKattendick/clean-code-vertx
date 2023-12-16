package at.technikum.vertx;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        CircuitBreaker cb = circuitBreaker();
        Retry retry = retry();
        RateLimiter rateLimiter = rateLimiter();

        Router router = Router.router(vertx);
        WebClient client = WebClient.create(vertx);

        router.get("/cb")
                .handler(
                        ctx -> VertxCircuitBreaker.executeFuture(cb, () ->
                                        client.get(8080, "localhost", "/does-not-exist")
                                                .as(BodyCodec.string())
                                                .expect(ResponsePredicate.SC_SUCCESS)
                                                .send()
                                )
                                .onSuccess(response -> ctx.end("Got: " + response.body() + "\n"))
                                .onFailure(error -> ctx.end("Failed with: " + error.toString() + "\n"))
                );

        router.get("/retry")
                .handler(
                        ctx -> VertxRetry.executeFuture(retry, vertx, () ->
                                        client.get(8080, "localhost", "/does-not-exist")
                                                .as(BodyCodec.string())
                                                .expect(ResponsePredicate.SC_SUCCESS)
                                                .send()
                                )
                                .onSuccess(response -> ctx.end("Got: " + response.body() + "\n"))
                                .onFailure(error -> ctx.end("Failed with: " + error.toString() + "\n"))
                );

        router.get("/limit")
                .handler(
                        ctx -> VertxRateLimiter.executeFuture(rateLimiter, vertx, 1, () ->
                                        client.get(8080, "localhost", "/does-not-exist")
                                                .as(BodyCodec.string())
                                                .expect(ResponsePredicate.SC_SUCCESS)
                                                .send()
                                )
                                .onSuccess(response -> ctx.end("Got: " + response.body() + "\n"))
                                .onFailure(error -> ctx.end("Failed with: " + error.toString() + "\n"))
                );

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(8080)
                .onSuccess(server -> System.out.println("HTTP server started on port " + server.actualPort()));
    }

    private CircuitBreaker circuitBreaker() {
        return CircuitBreaker.of("vertx-circuit-breaker", CircuitBreakerConfig.custom()
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(Duration.ofMillis(10_000))
                .build());
    }

    private Retry retry() {
        return Retry.of("vertx-retry", RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.of(100, ChronoUnit.MILLIS))
                .build());
    }

    private RateLimiter rateLimiter() {
        return RateLimiter.of("vertx-rate-limiter", RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMillis(1000))
                .limitForPeriod(1)
                .timeoutDuration(Duration.ofMillis(25))
                .build());
    }
}
