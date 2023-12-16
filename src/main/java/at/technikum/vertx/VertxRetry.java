package at.technikum.vertx;

import io.github.resilience4j.retry.Retry;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import lombok.AllArgsConstructor;
import lombok.experimental.UtilityClass;

import java.util.function.Supplier;

@UtilityClass
public class VertxRetry {

    public <T> Future<T> executeFuture(Retry retry, Vertx vertx, Supplier<Future<T>> supplier) {
        ContextInternal ctx = ContextInternal.current();
        Promise<T> promise = ctx != null ? ctx.promise() : Promise.promise();

        new AsyncRetryBlock<>(vertx, retry.asyncContext(), supplier, promise).run();

        return promise.future();
    }

    @AllArgsConstructor
    private static class AsyncRetryBlock<T> implements Runnable, Handler<Long> {

        private final Vertx vertx;
        private final Retry.AsyncContext<T> retryContext;
        private final Supplier<Future<T>> supplier;
        private final Promise<T> promise;

        @Override
        public void run() {
            try {
                supplier.get().onComplete(result -> {
                    if (result.failed()) {
                        if (result.cause() instanceof Exception e) {
                            onError(e);
                        } else {
                            promise.fail(result.cause());
                        }
                    } else {
                        onResult(result.result());
                    }
                });
            } catch (Exception e) {
                onError(e);
            }
        }

        @Override
        public void handle(Long ignored) {
            run();
        }

        private void onError(Exception t) {
            long delay = retryContext.onError(t);
            if (delay < 1) {
                promise.fail(t);
            } else {
                vertx.setTimer(delay, this);
            }
        }

        private void onResult(T result) {
            long delay = retryContext.onResult(result);
            if (delay < 1) {
                try {
                    retryContext.onComplete();
                    promise.complete(result);
                } catch (Exception e) {
                    promise.fail(e);
                }
            } else {
                vertx.setTimer(delay, this);
            }
        }
    }
}