import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stress test for RedisWorkerPool v2.
 *
 * Scenario
 * --------
 * workers = 4, queue = 8
 * clients = 50  (each simulates a slow Redis operation: 500 ms)
 *
 * At any moment only 4 workers are executing and 8 tasks can wait.
 * That means max 12 tasks can be in the system simultaneously.
 * The remaining ~38 should time out and receive TaskRejectedException.
 *
 * What to observe
 * ---------------
 * 1. Only 4 "running" lines appear at a time.
 * 2. Rejection messages for clients beyond queue capacity.
 * 3. PoolHealth transitions: HEALTHY → DEGRADED → OVERLOADED.
 * 4. producerWaitMs shows how long the lucky accepted producers waited.
 * 5. consumerWaitMs is low — workers are busy, not idle.
 *
 * Exact rejection count varies by scheduler timing — that's intentional.
 * Concurrency means timing matters.
 *
 * Part 2 — RouterStress
 * ----------------------
 * After the pool test, we fire 100 mixed commands through CommandRouter
 * to observe per-partition distribution, queue depths, and wait metrics
 * across all five data-type partitions.
 */
public class RedisPoolStressTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== PART 1: RedisWorkerPool stress (4 workers, queue=8, 50 clients) ===");
        System.out.println();
        runPoolStress();

        System.out.println();
        System.out.println("=== PART 2: CommandRouter stress (12 workers, 100 commands, 5 data types) ===");
        System.out.println();
        runRouterStress();
    }

    // ----------------------------------------------------------------
    // Part 1 — raw pool stress
    // ----------------------------------------------------------------

    private static void runPoolStress() throws Exception {

        RedisWorkerPool pool = new RedisWorkerPool(4, 8);

        // Background monitor prints a snapshot every second during the test.
        RedisPoolMonitor monitor = new RedisPoolMonitor(pool, 1_000);
        Thread monitorThread = new Thread(monitor, "stress-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();

        int clients            = 50;
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        long testStart = System.currentTimeMillis();

        for (int i = 0; i < clients; i++) {

            final int clientId = i;

            try {

                pool.submit(
                        () -> {
                            System.out.printf(
                                    "  [RUNNING ] client-%02d on %s%n",
                                    clientId,
                                    Thread.currentThread().getName()
                            );
                            try {
                                // Simulate slow work (e.g. a serialised write)
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            System.out.printf(
                                    "  [DONE    ] client-%02d%n", clientId);
                        },
                        100,                   // wait at most 100 ms for queue space
                        TimeUnit.MILLISECONDS
                );

                accepted.incrementAndGet();

            } catch (TaskRejectedException e) {

                System.out.printf(
                        "  [REJECTED] client-%02d — %s%n",
                        clientId, e.getMessage());
                rejected.incrementAndGet();
            }
        }

        System.out.println();
        System.out.printf("  Submitted: %d  Accepted: %d  Rejected: %d%n",
                clients, accepted.get(), rejected.get());
        System.out.println();

        // Let queued tasks drain before printing the final snapshot.
        Thread.sleep(4_000);

        System.out.println("--- Final pool metrics ---");
        System.out.println(pool.metrics());
        System.out.println();

        pool.shutdown();
        monitor.stop();
        System.out.println("Pool shut down cleanly.");
        System.out.printf("Total test time: %d ms%n",
                System.currentTimeMillis() - testStart);
    }

    // ----------------------------------------------------------------
    // Part 2 — CommandRouter / DataTypePartition stress
    // ----------------------------------------------------------------

    /**
     * Fires 100 simulated commands through CommandRouter, mixing all
     * five data types to show per-partition distribution and wait metrics.
     *
     * Each task sleeps 200 ms to give workers something measurable to do.
     * We use 20 different Redis keys so they spread across partitions.
     */
    private static void runRouterStress() throws Exception {

        CommandRouter router = new CommandRouter(8);

        // Mix of commands across all 5 data types
        String[][] commandTemplates = {
            // { verb, keyPrefix }
            { "SET",    "user"    },   // STRING
            { "GET",    "user"    },   // STRING
            { "INCR",   "counter" },   // STRING
            { "LPUSH",  "queue"   },   // LIST
            { "RPOP",   "queue"   },   // LIST
            { "HSET",   "profile" },   // HASH
            { "HGET",   "profile" },   // HASH
            { "SADD",   "tags"    },   // SET
            { "SREM",   "tags"    },   // SET
            { "ZADD",   "ranking" },   // ZSET
        };

        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        long routerStart = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {

            String[] template  = commandTemplates[i % commandTemplates.length];
            String   verb      = template[0];
            // 20 distinct keys → they spread across partition slots
            String   key       = template[1] + ":" + (i % 20);
            final    int taskId = i;
            final    String taskVerb = verb;
            final    String taskKey  = key;

            try {

                router.route(
                        verb,
                        key,
                        () -> {
                            // Simulate data-store work
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            System.out.printf(
                                    "  [DONE] task-%03d  %-6s  key=%-20s  thread=%s%n",
                                    taskId, taskVerb, taskKey,
                                    Thread.currentThread().getName()
                            );
                        },
                        150,
                        TimeUnit.MILLISECONDS
                );

                accepted.incrementAndGet();

            } catch (TaskRejectedException e) {

                System.out.printf(
                        "  [REJ ] task-%03d  %-6s  key=%-20s  — %s%n",
                        taskId, verb, key, e.getMessage());
                rejected.incrementAndGet();
            }
        }

        System.out.println();
        System.out.printf("  Routed: 100  Accepted: %d  Rejected: %d%n",
                accepted.get(), rejected.get());
        System.out.println();

        // Let tasks drain.
        Thread.sleep(5_000);

        System.out.println("--- Final router metrics ---");
        router.printMetrics();

        router.shutdown();
        System.out.println("Router shut down cleanly.");
        System.out.printf("Total router test time: %d ms%n",
                System.currentTimeMillis() - routerStart);
    }
}
