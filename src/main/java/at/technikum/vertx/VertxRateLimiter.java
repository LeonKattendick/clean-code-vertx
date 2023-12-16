package at.technikum.vertx;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import lombok.experimental.UtilityClass;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@UtilityClass
public class VertxRateLimiter {

    public <T> Future<T> executeFuture(RateLimiter rateLimiter, Vertx vertx, int permits, Supplier<Future<T>> supplier) {
        ContextInternal ctx = ContextInternal.current();
        Promise<T> promise = ctx != null ? ctx.promise() : Promise.promise();

        long delay = rateLimiter.reservePermission(permits);
        if (delay < 0) {
            promise.fail(RequestNotPermitted.createRequestNotPermitted(rateLimiter));
        } else if (delay == 0) {
            invokePermitted(promise, rateLimiter, supplier);
        } else {
            vertx.setTimer(TimeUnit.NANOSECONDS.toMillis(delay), ignored -> {
                invokePermitted(promise, rateLimiter, supplier);
            });
        }

        return promise.future();
    }

    private static <T> void invokePermitted(Promise<T> promise, RateLimiter rateLimiter, Supplier<Future<T>> supplier) {
        try {
            supplier.get().onComplete(result -> {
                if (result.failed()) {
                    rateLimiter.onError(result.cause());
                    promise.fail(result.cause());
                } else {
                    rateLimiter.onResult(result.result());
                    promise.complete(result.result());
                }
            });
        } catch (Exception e) {
            rateLimiter.onError(e);
            promise.fail(e);
        }
    }
}