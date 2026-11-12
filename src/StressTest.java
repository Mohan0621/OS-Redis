import java.util.ArrayList;
import java.util.List;

/**
 * Multi-phase stress test driver.
 *
 * <p>Runs three phases of concurrent {@link StressTestClient} instances
 * against a Redis-compatible server assumed to be already running on
 * localhost:6379.  Each phase spins up a fixed number of clients,
 * starts all threads simultaneously, waits for all to finish, then
 * prints phase and overall summaries.
 *
 * <p>Phase 1: 20 clients (ids  0 –  19)<br>
 * Phase 2: 50 clients (ids 20 –  69)<br>
 * Phase 3: 100 clients (ids 70 – 169)
 */
public class StressTest {

    public static void main(String[] args) {

        int[] phaseSizes = {20, 50, 100};

        long totalSuccess = 0;
        long totalErrors  = 0;
        int  totalClients = 0;
        int  clientIdOffset = 0;

        long overallStart = System.currentTimeMillis();

        try {

            for (int phase = 0; phase < phaseSizes.length; phase++) {

                int phaseNumber = phase + 1;
                int count       = phaseSizes[phase];

                System.out.println(
                        "\n=== Phase " + phaseNumber
                        + ": " + count + " concurrent clients ===");

                List<StressTestClient> clients = new ArrayList<>(count);
                List<Thread>           threads = new ArrayList<>(count);

                for (int i = 0; i < count; i++) {
                    int clientId = clientIdOffset + i;
                    StressTestClient client = new StressTestClient(clientId);
                    Thread thread = new Thread(client, "stress-client-" + clientId);
                    clients.add(client);
                    threads.add(thread);
                }

                long phaseStart = System.currentTimeMillis();

                // Start all threads first, then join all — no interleaving
                for (Thread t : threads) {
                    t.start();
                }

                for (Thread t : threads) {
                    t.join();
                }

                long phaseMs = System.currentTimeMillis() - phaseStart;

                long phaseSuccess = 0;
                long phaseErrors  = 0;

                for (StressTestClient client : clients) {
                    phaseSuccess += client.getSuccessCount();
                    phaseErrors  += client.getErrorCount();
                }

                System.out.println(
                        "Phase " + phaseNumber
                        + " done in " + phaseMs + " ms"
                        + " | clients=" + count
                        + " | success=" + phaseSuccess
                        + " | errors=" + phaseErrors);

                totalSuccess  += phaseSuccess;
                totalErrors   += phaseErrors;
                totalClients  += count;
                clientIdOffset += count;
            }

        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }

        long totalMs = System.currentTimeMillis() - overallStart;

        System.out.println("\n=== Overall Summary ===");
        System.out.println("Total clients    : " + totalClients);
        System.out.println("Total commands   : " + (totalSuccess + totalErrors));
        System.out.println("Total successes  : " + totalSuccess);
        System.out.println("Total errors     : " + totalErrors);
        System.out.println("Total time (ms)  : " + totalMs);
    }
}
