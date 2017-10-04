/**
 * Copyright 2016-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.reporter.amqp;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.codec.Encoding;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Sender;
import zipkin2.reporter.internal.BaseCall;

import static java.util.logging.Level.WARNING;

/**
 * This sends (usually json v2) encoded spans to a RabbitMQ queue.
 *
 * <p>This sender is thread-safe.
 */
@AutoValue
public abstract class RabbitMQSender extends Sender {

  public static RabbitMQSender addresses(String addresses) {
    return newBuilder().addresses(addresses).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** Configuration including defaults needed to send spans to a RabbitMQ queue. */
  public static final class Builder {
    String addresses;
    String queue = "zipkin";
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 100000;

    /** Comma-separated list of host:port pairs */
    public Builder addresses(String addresses) {
      if (addresses == null) throw new NullPointerException("addresses == null");
      this.addresses = addresses;
      return this;
    }

    /** Queue zipkin spans will be sent to. Defaults to "zipkin" */
    public Builder queue(String queue) {
      if (queue == null) throw new NullPointerException("queue == null");
      this.queue = queue;
      return this;
    }

    public Builder encoding(Encoding encoding) {
      if (encoding == null) throw new NullPointerException("encoding == null");
      this.encoding = encoding;
      return this;
    }

    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    public final RabbitMQSender build() {
      return new AutoValue_RabbitMQSender(
          encoding,
          messageMaxBytes,
          addresses,
          queue,
          BytesMessageEncoder.forEncoding(encoding)
      );
    }
  }

  public final Builder toBuilder() {
    return new Builder().addresses(addresses()).queue(queue()).encoding(encoding())
        .messageMaxBytes(messageMaxBytes());
  }

  abstract String addresses();

  abstract String queue();

  abstract BytesMessageEncoder encoder();

  /** get and close are typically called from different threads */
  volatile boolean provisioned, closeCalled;

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  /**
   * The returned call will send all of the spans as a single message.
   */
  @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new IllegalStateException("closed");
    byte[] message = encoder().encode(encodedSpans);
    return new RabbitMQCall(message);
  }

  /** Ensures there are no connection issues. */
  @Override public CheckResult check() {
    try {
      get().isOpen();
      return CheckResult.OK;
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Memoized Connection get() {
    Connection result;
    try {
      result = new ConnectionFactory().newConnection(convertAddresses(addresses()));
    } catch (IOException | TimeoutException e) {
      throw new IllegalStateException("Unable to establish connection to RabbitMQ server", e);
    }
    provisioned = true;
    return result;
  }

  @Override public synchronized void close() throws IOException {
    if (closeCalled) return;
    if (provisioned) get().close();
    closeCalled = true;
  }

  class RabbitMQCall extends BaseCall<Void> { // RabbitMQFuture is not cancelable
    private final Logger logger = Logger.getLogger(RabbitMQCall.class.getName());
    private final byte[] message;

    RabbitMQCall(byte[] message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      publish();
      return null;
    }

    void publish() throws IOException {
      Channel channel = get().createChannel();
      try {
        channel.basicPublish("", queue(), null, message);
      } finally {
        try {
          channel.close();
        } catch (TimeoutException e) {
          if (logger.isLoggable(WARNING)) {
            logger.log(WARNING, "Timed out closing RabbitMQ publisher channel. "
                + "Channel may still be open", e);
          }
        }
      }
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        publish();
        callback.onSuccess(null);
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<Void> clone() {
      return new RabbitMQCall(message);
    }
  }

  static Address[] convertAddresses(String addresses) {
    String[] addressStrings = addresses.split(",");
    Address[] addressArray = new Address[addressStrings.length];
    for (int i = 0; i < addressStrings.length; i++) {
      String[] splitAddress = addressStrings[i].split(":");
      String host = splitAddress[0];
      Integer port = null;
      try {
        if (splitAddress.length == 2) port = Integer.parseInt(splitAddress[1]);
      } catch (NumberFormatException ignore) {
      }
      addressArray[i] = (port != null) ? new Address(host, port) : new Address(host);
    }
    return addressArray;
  }
}
