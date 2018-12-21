package io.zonky.test.db.provider;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.ListenableFutureTask;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.toList;
import static org.springframework.core.Ordered.HIGHEST_PRECEDENCE;

public class PrefetchingDatabaseProvider implements GenericDatabaseProvider {

    private static final Logger logger = LoggerFactory.getLogger(PrefetchingDatabaseProvider.class);

    private static final ConcurrentMap<PipelineKey, DatabasePipeline> pipelines = new ConcurrentHashMap<>();
    private static final PriorityThreadPoolTaskExecutor taskExecutor = new PriorityThreadPoolTaskExecutor();
    private static final AtomicInteger pipelineCacheSize = new AtomicInteger(1);

    static {
        taskExecutor.setThreadNamePrefix("prefetching-");
        taskExecutor.setAllowCoreThreadTimeOut(true);
        taskExecutor.setKeepAliveSeconds(60);
        taskExecutor.setCorePoolSize(1);
        taskExecutor.initialize();
    }

    private final GenericDatabaseProvider delegate;

    public PrefetchingDatabaseProvider(GenericDatabaseProvider delegate, Environment environment) {
        this.delegate = delegate;

        String threadNamePrefix = environment.getProperty("embedded-database.prefetching.thread-name-prefix", "prefetching-");
        int concurrency = environment.getProperty("embedded-database.prefetching.concurrency", int.class, 3);
        int cacheSize = environment.getProperty("embedded-database.prefetching.pipeline-cache-size", int.class, 3);

        taskExecutor.setThreadNamePrefix(threadNamePrefix);
        taskExecutor.setCorePoolSize(concurrency);

        pipelineCacheSize.set(cacheSize);
    }

    @Override
    public DataSource getDatabase(DatabasePreparer preparer, DatabaseDescriptor descriptor) throws InterruptedException {
        pipelines.values().forEach(d -> System.out.println("Pipeline: " + d)); // TODO

        Stopwatch callStopwatch = Stopwatch.createStarted(); // TODO
        Stopwatch takeStopwatch = null; // TODO
        try {
            PipelineKey key = new PipelineKey(preparer, descriptor);
            DatabasePipeline pipeline = pipelines.computeIfAbsent(key, k -> new DatabasePipeline());
            prepareDatabase(key, HIGHEST_PRECEDENCE);

            long invocationCount = pipeline.requests.incrementAndGet();
            if (invocationCount == 1) {
                int cacheSize = pipelineCacheSize.get();
                for (int i = 1; i <= cacheSize; i++) {
                    int priority = -1 * (int) (invocationCount / cacheSize * i);
                    prepareDatabase(key, priority);
                }
            } else {
                Stopwatch syncStopwatch = Stopwatch.createStarted(); // TODO
                synchronized (pipeline.tasks) {
                    List<PriorityFutureTask> cancelledTasks = pipeline.tasks.stream()
                            .filter(t -> t.priority > HIGHEST_PRECEDENCE)
                            .filter(t -> t.cancel(false))
                            .collect(toList());

                    for (int i = 1; i <= cancelledTasks.size(); i++) {
                        int priority = -1 * (int) (invocationCount / cancelledTasks.size() * i);
                        prepareDatabase(key, priority);
                    }
                }
                System.out.println("Sync Duration: " + syncStopwatch); // TODO
            }

            takeStopwatch = Stopwatch.createStarted();
            System.out.println("Result Size: " + pipeline.results.size()); // TODO
            return pipeline.results.take();
        } finally {
            System.out.println("Take Duration: " + takeStopwatch); // TODO
            System.out.println("Call Duration: " + callStopwatch); // TODO
        }
    }

    private ListenableFutureTask<DataSource> prepareDatabase(PipelineKey key, int priority) {
        DatabasePipeline pipeline = pipelines.get(key);

        PriorityFutureTask<DataSource> task = new PriorityFutureTask<>(() -> {
            for (int i = 2; i >= 0; i--) {
                try {
                    return delegate.getDatabase(key.preparer, key.descriptor);
                } catch (Exception e) {
                    if (i == 0) {
                        throw e;
                    }
                }
            }
            return null;
        }, priority);

        task.addCallback(new ListenableFutureCallback<DataSource>() {
            @Override
            public void onSuccess(DataSource result) {
                pipeline.tasks.remove(task);
                pipeline.results.offer(result);
            }

            @Override
            public void onFailure(Throwable ex) {
                pipeline.tasks.remove(task);
                if (ex instanceof ExecutionException) {
                    logger.error("Unexpected error while preparing a new database", ex);
                }
            }
        });

        pipeline.tasks.add(task);
        taskExecutor.execute(task);
        return task;
    }

    private static class PipelineKey {

        private final DatabasePreparer preparer;
        private final DatabaseDescriptor descriptor;

        private PipelineKey(DatabasePreparer preparer, DatabaseDescriptor descriptor) {
            this.preparer = preparer;
            this.descriptor = descriptor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PipelineKey that = (PipelineKey) o;
            return Objects.equals(preparer, that.preparer) &&
                    Objects.equals(descriptor, that.descriptor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(preparer, descriptor);
        }
    }

    private static class DatabasePipeline {

        private final AtomicLong requests = new AtomicLong();
        private final Set<PriorityFutureTask> tasks = newSetFromMap(new ConcurrentHashMap<>());
        private final BlockingQueue<DataSource> results = new LinkedBlockingQueue<>();

        @Override
        public String toString() { // TODO
            return MoreObjects.toStringHelper(this)
                    .add("requestCount", requests.get())
                    .add("taskCount", tasks.size())
                    .add("resultCount", results.size())
                    .toString();
        }
    }

    private static class PriorityThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {

        @Override
        protected BlockingQueue<Runnable> createQueue(int queueCapacity) {
            return new PriorityBlockingQueue<>();
        }
    }

    private static class PriorityFutureTask<T> extends ListenableFutureTask<T> implements Comparable<PriorityFutureTask> {

        private final int priority;

        public PriorityFutureTask(Callable<T> callable, int priority) {
            super(callable);
            this.priority = priority;
        }

        @Override
        public int compareTo(PriorityFutureTask task) {
            return Integer.compare(priority, task.priority);
        }
    }
}
