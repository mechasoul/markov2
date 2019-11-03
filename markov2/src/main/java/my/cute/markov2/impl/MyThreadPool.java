package my.cute.markov2.impl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public enum MyThreadPool implements ExecutorService {

	INSTANCE;
	
	private static class MyForkJoinWorkerThread extends ForkJoinWorkerThread {

		protected MyForkJoinWorkerThread(ForkJoinPool pool) {
			super(pool);
		}
		
	}
	
	private final ExecutorService e = new ForkJoinPool(Runtime.getRuntime().availableProcessors(), 
			new ForkJoinPool.ForkJoinWorkerThreadFactory() {
		
		@Override
		public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
			ForkJoinWorkerThread thread = new MyForkJoinWorkerThread(pool);
			thread.setDaemon(false);
			return thread;
		}
	}, Thread.getDefaultUncaughtExceptionHandler(), true);

	@Override
	public void execute(Runnable arg0) {
		e.execute(arg0);
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return e.awaitTermination(timeout, unit);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return e.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return e.invokeAll(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return e.invokeAny(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return e.invokeAny(tasks);
	}

	@Override
	public boolean isShutdown() {
		return e.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return e.isTerminated();
	}

	@Override
	public void shutdown() {
		e.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return e.shutdownNow();
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return e.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return e.submit(task, result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return e.submit(task);
	}

	
	
	
}
