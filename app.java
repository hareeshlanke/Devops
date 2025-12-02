
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A complex Java 17 demo:
 *  - Asynchronous pipeline using CompletableFuture
 *  - Rate limiter (token bucket)
 *  - Circuit breaker with HALF_OPEN state
 *  - Exponential backoff retry
 *  - LRU cache to deduplicate calls
 *  - Structured logging and graceful shutdown
 */
public class ComplexPipelineDemo {

    // ----- Domain -----
    record Task(String id, String payload) {}

    // Simulates an external service that sometimes fails and has variable latency.
    static class ExternalService {
        private final Random rnd = new Random();
        private final String name;

        ExternalService(String name) { this.name = name; }

        // May throw exceptions to trigger retries/circuit breaker
        public String call(String key) {
            // Simulate variable latency (50–200 ms)
            sleepMillis(50 + rnd.nextInt(150));

            // Simulate 25% failure rate
            if (rnd.nextDouble() < 0.25) {
                throw new RuntimeException("Service " + name + " transient failure for key=" + key);
            }

            // Return some computed payload
            return "Result[" + name + "]::" + key.toUpperCase(Locale.ROOT);
        }
    }

    // ----- Rate Limiter (Token Bucket) -----
    static class RateLimiter implements AutoCloseable {
        private final int permitsPerSecond;
        private final int maxBurst;
        private final Semaphore semaphore;
        private final ScheduledExecutorService scheduler;
        private final long periodMs;
        private final double tokensPerTick;
        private double accumulator = 0.0;

        public RateLimiter(int permitsPerSecond, int maxBurst) {
            if (permitsPerSecond <= 0 || maxBurst <= 0) {
                throw new IllegalArgumentException("permitsPerSecond and maxBurst must be > 0");
            }
            this.permitsPerSecond = permitsPerSecond;
            this.maxBurst = Math.max(maxBurst, permitsPerSecond);
            this.semaphore = new Semaphore(this.maxBurst);
            // Start full bucket
            this.semaphore.drainPermits();
            this.semaphore.release(this.maxBurst);

            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "rate-limiter-refill");
                t.setDaemon(true);
                return t;
            });

            // Refill in small ticks (e.g., 50ms)
            this.periodMs = 50;
            this.tokensPerTick = permitsPerSecond * (periodMs / 1000.0);

            scheduler.scheduleAtFixedRate(this::refill, periodMs, periodMs, TimeUnit.MILLISECONDS);
        }

        private void refill() {
            synchronized (this) {
                accumulator += tokensPerTick;
                int toAdd = (int) Math.floor(accumulator);
                if (toAdd > 0) {
                    accumulator -= toAdd;
                    int capacityLeft = maxBurst - semaphore.availablePermits();
                    int add = Math.min(toAdd, capacityLeft);
                    if (add > 0) {
                        semaphore.release(add);
                    }
                }
            }
        }

        public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            return semaphore.tryAcquire(1, timeout, unit);
        }

        @Override
        public void close() {
            scheduler.shutdownNow();
        }
    }

    // ----- Circuit Breaker -----
    static class CircuitBreaker {
        enum State { CLOSED, OPEN, HALF_OPEN }

        private final int failureThreshold;            // e.g., after N consecutive failures, open
        private final long openTimeoutMs;             // time to stay OPEN before HALF_OPEN
        private final int halfOpenMaxTrials;          // allow N trial requests in HALF_OPEN
        private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

        private volatile State state = State.CLOSED;
        private volatile long openedAt = 0;
        private final AtomicInteger halfOpenTrials = new AtomicInteger(0);

        public CircuitBreaker(int failureThreshold, Duration openTimeout, int halfOpenMaxTrials) {
            this.failureThreshold = failureThreshold;
            this.openTimeoutMs = openTimeout.toMillis();
            this.halfOpenMaxTrials = halfOpenMaxTrials;
        }

        public synchronized boolean allowRequest() {
            switch (state) {
                case CLOSED:
                    return true;
                case OPEN:
                    if (System.currentTimeMillis() - openedAt >= openTimeoutMs) {
                        state = State.HALF_OPEN;
                        halfOpenTrials.set(0);
                        return true; // allow limited trials
                    }
                    return false;
                case HALF_OPEN:
                    // Allow limited trial requests
                    if (halfOpenTrials.incrementAndGet() <= halfOpenMaxTrials) {
                        return true;
                    }
                    return false;
                default:
                    return false;
            }
        }

        public synchronized void recordSuccess() {
            consecutiveFailures.set(0);
            if (state == State.HALF_OPEN) {
                // Success during HALF_OPEN -> back to CLOSED
                state = State.CLOSED;
                halfOpenTrials.set(0);
            }
        }

        public synchronized void recordFailure() {
            int failures = consecutiveFailures.incrementAndGet();
            if (state == State.HALF_OPEN) {
                // Failure during HALF_OPEN -> go back to OPEN
                open();
            } else if (state == State.CLOSED && failures >= failureThreshold) {
                open();
            }
        }

        private void open() {
            state = State.OPEN;
            openedAt = System.currentTimeMillis();
        }

        public State state() { return state; }
    }

    // ----- LRU Cache -----
    static class LruCache<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        public LruCache(int capacity) {
            super(capacity, 0.75f, true); // access-order for LRU
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > capacity;
        }

        public synchronized V getValue(K key) { return super.get(key); }
        public synchronized V putValue(K key, V val) { return super.put(key, val); }
        public synchronized boolean containsKeyValue(K key) { return super.containsKey(key); }
    }

    // ----- Retry with Exponential Backoff -----
    static class Retry {
        public static <T> T withExponentialBackoff(Supplier<T> supplier,
                                                   int maxRetries,
                                                   Duration initialDelay,
                                                   double multiplier) {
            int attempt = 0;
            Duration delay = initialDelay;
            while (true) {
                try {
                    return supplier.get();
                } catch (Exception e) {
                    attempt++;
                    if (attempt > maxRetries) {
                        throw e;
                    }
                    sleepMillis(delay.toMillis());
                    delay = Duration.ofMillis(Math.min(
                            (long)(delay.toMillis() * multiplier),
                            10_000L)); // cap delay at 10s
                }
            }
        }
    }

    // ----- Utility -----
    private static void sleepMillis(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    // ----- Pipeline -----
    public static void main(String[] args) throws Exception {
        System.out.println("Started at: " + Instant.now());

        // Thread pools
        ExecutorService ioPool = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                r -> {
                    Thread t = new Thread(r, "io-pool");
                    t.setDaemon(true);
                    return t;
                }
        );

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        // Building blocks
        try (RateLimiter rateLimiter = new RateLimiter(permitsPerSecond(), 10)) {
            CircuitBreaker breaker = new CircuitBreaker(3, Duration.ofSeconds(3), 2);
            ExternalService service = new ExternalService("alpha");
            LruCache<String, String> cache = new LruCache<>(128);

            // Prepare some tasks with duplicates to show caching
            List<Task> tasks = List.of(
                    new Task("t1", "foo"),
                    new Task("t2", "bar"),
                    new Task("t3", "foo"),    // duplicate payload
                    new Task("t4", "baz"),
                    new Task("t5", "bar")     // duplicate payload
            );

            List<CompletableFuture<String>> futures = new ArrayList<>();

            for (Task task : tasks) {
                CompletableFuture<String> fut =
                        CompletableFuture.supplyAsync(() -> preprocess(task), ioPool)
                                .thenCompose(pre -> processAsync(pre, breaker, rateLimiter, cache, service, ioPool))
                                .thenApply(ComplexPipelineDemo::postprocess)
                                .orTimeout(2, TimeUnit.SECONDS) // protect against long stalls
                                .exceptionally(ex -> {
                                    log("ERROR", Map.of("taskId", task.id(), "error", ex.toString()));
                                    return "FAILED(" + task.id() + ")";
                                });

                futures.add(fut);
            }

            // Collect results
            List<String> results = futures.stream()
                    .map(CompletableFuture::join)
                    .toList();

            System.out.println("\n--- Final Results ---");
            results.forEach(System.out::println);
        } finally {
            scheduler.shutdownNow();
            ioPool.shutdownNow();
            System.out.println("\nShutdown complete at: " + Instant.now());
        }
    }

    private static int permitsPerSecond() {
        // You can tune this to see rate limiter behavior.
        return 5;
    }

    // Stage 1: Preprocess (CPU-bound)
    private static String preprocess(Task task) {
        // Example transforming payload (could be parsing, validation, enrich, etc.)
        String normalized = task.payload().trim().toLowerCase(Locale.ROOT);
        log("PREPROCESS", Map.of("taskId", task.id(), "payload", task.payload(), "normalized", normalized));
        return normalized;
    }

    // Stage 2: Async processing with cache, rate limiter, circuit breaker, retry
    private static CompletableFuture<String> processAsync(
            String key,
            CircuitBreaker breaker,
            RateLimiter limiter,
            LruCache<String, String> cache,
            ExternalService service,
            Executor executor
    ) {
        return CompletableFuture.supplyAsync(() -> {
            // Cache hit short-circuit
            if (cache.containsKeyValue(key)) {
                String cached = cache.getValue(key);
                log("CACHE_HIT", Map.of("key", key, "value", cached));
                return cached;
            }

            // Circuit breaker gate
            if (!breaker.allowRequest()) {
                log("CIRCUIT_OPEN", Map.of("key", key, "state", breaker.state().name()));
                throw new RuntimeException("CircuitOpen: rejecting key=" + key);
            }

            // Rate limiting
            try {
                if (!limiter.tryAcquire(500, TimeUnit.MILLISECONDS)) {
                    log("RATE_LIMIT_BLOCKED", Map.of("key", key));
                    throw new RuntimeException("RateLimited: timeout acquiring permit for key=" + key);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while rate-limiting", e);
            }

            // Retry with exponential backoff
            try {
                String result = Retry.withExponentialBackoff(
                        () -> service.call(key),
                        3,
                        Duration.ofMillis(100),
                        2.0
                );
                // Success flows through circuit breaker and cache
                breaker.recordSuccess();
                cache.putValue(key, result);
                log("SERVICE_OK", Map.of("key", key, "result", result, "cbState", breaker.state().name()));
                return result;
            } catch (Exception e) {
                breaker.recordFailure();
                log("SERVICE_FAIL", Map.of("key", key, "error", e.toString(), "cbState", breaker.state().name()));
                throw e;
            }
        }, executor);
    }

    // Stage 3: Postprocess
    private static String postprocess(String s) {
        String out = "Processed<" + s + ">";
        log("POSTPROCESS", Map.of("out", out));
        return out;
    }

    // Structured logging helper
    private static void log(String event, Map<String, Object> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(event).append(']').append(' ');
        sb.append("ts=").append(Instant.now());
        for (Map.Entry<String, Object> e : fields.entrySet()) {
            sb.append(' ').append(e.getKey()).append('=').append(e.getValue());
        }
        System.out.println(sb);
    }
}

