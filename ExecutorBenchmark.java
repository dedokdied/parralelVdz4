import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class ExecutorBenchmark {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    enum TaskMode {
        SLEEP,
        BLACK_HOLE
    }

    interface Shutdownable {
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, Executor {}

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {
        List<Thread> threads = new ArrayList<>();

        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }

        @Override
        public void shutdown() {
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {
        protected static final Runnable EXIT_TASK = () -> {};

        AtomicInteger counter = new AtomicInteger(0);
        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threads) {
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x -> x < threads, x -> x + 1);

            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x -> new LinkedBlockingDeque<Runnable>())
                            .toList());

            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();

            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task;
                    try {
                        task = tasks.get(id).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    if (task == EXIT_TASK) {
                        return;
                    }

                    task.run();
                }
            });
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }

            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown) {
                throw new RejectedExecutionException("Executor is shutting down");
            }

            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {
        WorkStealingExecutor(int threads) {
            super(threads);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();

                    if (task == null) {
                        task = stealTask(id);
                    }

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }

                        if (task == null) {
                            continue;
                        }
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);

                        if (task == null) {
                            return;
                        }

                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null) {
                    return task;
                }
            }

            return null;
        }
    }

    static class FixedThreadPoolWrapper implements ShutdownableExecutor {
        private final ExecutorService pool;
        private final List<Future<?>> futures = new ArrayList<>();

        FixedThreadPoolWrapper(int threadNumber) {
            pool = Executors.newFixedThreadPool(threadNumber);
        }

        @Override
        public synchronized void execute(Runnable command) {
            futures.add(pool.submit(command));
        }

        @Override
        public synchronized void shutdown() {
            for (Future<?> future : futures) {
                try {
                    future.get(600, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    pool.shutdownNow();
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    pool.shutdownNow();
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    pool.shutdownNow();
                    throw new RuntimeException(e);
                }
            }

            pool.shutdownNow();
        }
    }

    static volatile long BLACK_HOLE_SINK;

    static final long BLACK_HOLE_MULTIPLIER = 1_000_000L;

    static long blackHole(long difficulty) {
        long x = 0;

        for (long i = 0; i < difficulty; i++) {
            x = x * 31 + i;
        }

        return x;
    }

    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 200;
    static final int TARGET_OPTIMAL_FULL_TIME = 1000;

    static final int MEAN_TASK_TIME =
            (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER);

    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;

    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    static final int WARMUP_TASK_NUMBER = 500;

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);

            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_TIME * THREAD_NUMBER
                    : 0;

            case PARETO -> {
                var scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        var durations = new int[TASK_NUMBER];

        long totalDuration = 0;

        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }

        if (totalDuration == 0) {
            totalDuration = 1;
        }

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;

        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }

        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }

        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;

            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }

        return durations;
    }
    static Runnable createTask(int duration, TaskMode mode) {
        if (mode == TaskMode.SLEEP) {
            return () -> {
                try {
                    Thread.sleep(duration);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
        }

        long difficulty = (long) duration * BLACK_HOLE_MULTIPLIER;

        return () -> {
            BLACK_HOLE_SINK = blackHole(difficulty);
        };
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution, TaskMode mode) {
        var durations = createTaskDurations(distribution);

        return new Vector<>(
                IntStream
                        .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                        .mapToObj(x -> createTask(durations[x], mode))
                        .toList());
    }

    static void warmUp(ShutdownableExecutor executor, TaskMode mode) {
        for (int i = 0; i < WARMUP_TASK_NUMBER; i++) {
            executor.execute(createTask(1, mode));
        }

        executor.shutdown();
    }

    static void warmUpAll() {
        for (var mode : TaskMode.values()) {
            warmUp(new ThreadPerTaskExecutor(), mode);
            warmUp(new FixedThreadPoolWrapper(THREAD_NUMBER), mode);
            warmUp(new RoundRobinExecutor(THREAD_NUMBER), mode);
            warmUp(new WorkStealingExecutor(THREAD_NUMBER), mode);
        }
    }


    public record Pair<A, B>(A first, B second) {}

    public static Pair<Long, Long> measureExecutor(
            ShutdownableExecutor executor,
            TaskDistribution distribution,
            TaskMode mode) {

        var tasks = createTasks(distribution, mode);

        var start = System.nanoTime();

        tasks.forEach(executor::execute);

        var submissionEnd = System.nanoTime();

        executor.shutdown();

        var finish = System.nanoTime();

        return new Pair<>(submissionEnd - start, finish - start);
    }

    /*
    Результаты лежат в readme файле
    */
    public static void main(String[] args) {
        System.out.println("Applied feedback points: 1, 2, 3");
        System.out.println("THREAD_NUMBER = " + THREAD_NUMBER);
        System.out.println("TASK_NUMBER = " + TASK_NUMBER);
        System.out.println("TARGET_OPTIMAL_FULL_TIME = " + TARGET_OPTIMAL_FULL_TIME);
        System.out.println("BLACK_HOLE_MULTIPLIER = " + BLACK_HOLE_MULTIPLIER);
        System.out.println("WARMUP_TASK_NUMBER = " + WARMUP_TASK_NUMBER);
        System.out.println();

        System.out.println("Warm-up started");
        warmUpAll();
        System.out.println("Warm-up done");
        System.out.println();

        for (var mode : TaskMode.values()) {
            System.out.println("Mode: " + mode);

            for (var distribution : TaskDistribution.values()) {
                System.out.println("Task distribution: " + distribution);

                var threadPerTaskResult =
                        measureExecutor(new ThreadPerTaskExecutor(), distribution, mode);
                System.out.println("ThreadPerTask submission time: "
                        + threadPerTaskResult.first() / 1_000_000d);
                System.out.println("ThreadPerTask execution time: "
                        + threadPerTaskResult.second() / 1_000_000d);

                var fixedThreadPoolResult =
                        measureExecutor(new FixedThreadPoolWrapper(THREAD_NUMBER), distribution, mode);
                System.out.println("FixedThreadPool submission time: "
                        + fixedThreadPoolResult.first() / 1_000_000d);
                System.out.println("FixedThreadPool execution time: "
                        + fixedThreadPoolResult.second() / 1_000_000d);

                var roundRobinResult =
                        measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution, mode);
                System.out.println("RoundRobin submission time: "
                        + roundRobinResult.first() / 1_000_000d);
                System.out.println("RoundRobin execution time: "
                        + roundRobinResult.second() / 1_000_000d);

                var workStealingResult =
                        measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution, mode);
                System.out.println("WorkStealing submission time: "
                        + workStealingResult.first() / 1_000_000d);
                System.out.println("WorkStealing execution time: "
                        + workStealingResult.second() / 1_000_000d);

                System.out.println();
            }
        }

        System.out.println("BLACK_HOLE_SINK = " + BLACK_HOLE_SINK);
    }
}
