package zipkin2.reporter.internal;

import brave.*;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class DeadlockTest {

  static final int MAX_BYTES = 512 * 1024;

  /*
      Reporter is in the middle of draining a batch of pending spans.
      While this is happening, application threads report additional spans.
      This reporting gets blocked as the lock is held while draining pending spans happens.
      Application threads that are waiting on the lock can get parked during this time.
      Virtual threads that are pinned to carrier threads will not release the carrier threads.
      Virtual threads that are not pinned will not be bound to a specific carrier thread.
      When the lock is released after draining pending spans, it will be available for one of the waiting threads.
      If the thread that gets the lock next is a virtual thread that isn't pinned, it won't have a carrier thread to run on.
      The application becomes deadlocked.

      The obvious (though "unfair") solution would be for the lock to not be given to unpinned threads if there are pinned threads waiting.
      That could, of course, lead to starvation issues for unpinned threads, but it would avoid deadlocking the whole application.
       */
  @Test
  void deadlock() {
    String name = "you";
    name.isEmpty();
    FakeSender sender = FakeSender.create();
//    BufferNextMessage<MutableSpan> delegate = BufferNextMessage.create(Encoding.JSON, MAX_BYTES, TimeUnit.SECONDS.toNanos(1));
//    CountDownLatch latch = new CountDownLatch(1);
//    BufferNextMessage<MutableSpan> bufferNextMessage = new BufferNextMessage<>(MAX_BYTES, TimeUnit.SECONDS.toNanos(1)) {
//      @Override
//      int messageSizeInBytes(int nextSizeInBytes) {
//        return delegate.messageSizeInBytes(nextSizeInBytes);
//      }
//
//      @Override
//      void resetMessageSizeInBytes() {
//        delegate.resetMessageSizeInBytes();
//      }
//
//      @Override
//      public boolean offer(MutableSpan next, int nextSizeInBytes) {
//        try {
//          latch.await(5, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//          Assertions.fail(e);
//        }
//        return super.offer(next, nextSizeInBytes);
//      }
//    };
    AsyncZipkinSpanHandler spanHandler = AsyncZipkinSpanHandler.create(sender);
    Tracer tracer = Tracing.newBuilder()
      .currentTraceContext(ThreadLocalCurrentTraceContext.create())
      .sampler(Sampler.ALWAYS_SAMPLE)
      .addSpanHandler(spanHandler)
      .build().tracer();

    Runnable endSpan = () -> {
      Span span = tracer.nextSpan();
      span.finish();
    };

    Runnable endScopedSpan = () -> {
      ScopedSpan span = tracer.startScopedSpan("unpinned span");
      span.finish();
    };

    // make sure there is a span to report so doDrain is called while holding lock
    endSpan.run();

    // This needs to be the thread that gets the lock once the reporter thread releases the lock
    Thread unpinnedThread = Thread.ofVirtual().name("unpinned").start(endScopedSpan);
//    Thread unpinnedThread = Thread.ofVirtual().name("\"unpinned\"").start(endSpan);
    // These will block carrier threads leaving no carrier threads to mount the unpinned thread on when it tries to acquire the lock
    List<Thread> pinnedThreads = IntStream.range(0, Runtime.getRuntime().availableProcessors()).mapToObj(i -> Thread.ofVirtual().name("pinning-" + i).start(endSpan)).toList();

    // release the lock after all virtual threads have been parked waiting
    CountBoundedQueue.latch.countDown();

    Stream.concat(Stream.of(unpinnedThread), pinnedThreads.stream()).forEach(thread -> {
      try {
        if (!thread.join(Duration.ofSeconds(3))) {
          Assertions.fail("Deadlock detected");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static class FakeSender extends BytesMessageSender.Base {

    static FakeSender create() {
      return new FakeSender(Encoding.JSON);
    }

    private FakeSender(Encoding encoding) {
      super(encoding);
    }

    @Override
    public int messageMaxBytes() {
      return MAX_BYTES;
    }

    @Override
    public void send(List<byte[]> list) throws IOException {
      // no-op
    }

    @Override
    public void close() throws IOException {
    }
  }
}
