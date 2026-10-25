package demo;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A checkout pipeline: orders are validated, matched with their payments, enriched with stock from the inventory
 * service over HTTP, and summed per customer. Invalid orders go to a dead-letter sink.
 *
 * <p>The inventory service is the external dependency real pipelines have: when it slows down, the enrichment
 * runs out of requests in flight and back-pressures everything before it; when it fails or is gone, requests fail
 * and the job restarts.
 *
 * <p>Arguments: --rate orders per second (200), --inventory base URL (http://inventory:8080), --capacity requests
 * in flight per subtask (20), --timeout seconds (5).
 */
public final class Checkout {

    /** An order; amount is negative for an invalid one. */
    public static class Order {
        public long id;
        public String customer;
        public String sku;
        public double amount;
        public int stock;

        public Order() {}
    }

    /** The payment for an order. */
    public static class Payment {
        public long orderId;
        public double amount;

        public Payment() {}
    }

    public static void main(String[] args) throws Exception {
        Args a = new Args(args);
        long rate = a.integer("rate", 200);
        String inventory = a.string("inventory", "http://inventory:8080");
        int capacity = (int) a.integer("capacity", 20);
        long timeout = a.integer("timeout", 5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Order> orders = env.fromSource(
                new DataGeneratorSource<>(Checkout::order, Long.MAX_VALUE, RateLimiterStrategy.perSecond(rate),
                        TypeInformation.of(Order.class)),
                WatermarkStrategy.noWatermarks(), "Orders");
        DataStream<Payment> payments = env.fromSource(
                new DataGeneratorSource<>(Checkout::payment, Long.MAX_VALUE, RateLimiterStrategy.perSecond(rate),
                        TypeInformation.of(Payment.class)),
                WatermarkStrategy.noWatermarks(), "Payments");

        OutputTag<Order> invalid = new OutputTag<>("invalid") {};
        SingleOutputStreamOperator<Order> valid = orders.process(new Validate(invalid)).name("Validate orders");
        valid.getSideOutput(invalid).sinkTo(new DiscardingSink<>()).name("Dead letters");

        DataStream<Order> paid = valid.keyBy(o -> o.id)
                .connect(payments.keyBy(p -> p.orderId))
                .process(new MatchPayments())
                .name("Match payments");

        DataStream<Order> enriched = AsyncDataStream
                .unorderedWait(paid, new EnrichFromInventory(inventory), timeout, TimeUnit.SECONDS, capacity)
                .name("Enrich from inventory");
        enriched.sinkTo(new DiscardingSink<>()).name("Orders warehouse");

        enriched.keyBy(o -> o.customer)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .reduce((x, y) -> {
                    x.amount += y.amount;
                    return x;
                })
                .name("Revenue per customer")
                .sinkTo(new DiscardingSink<>())
                .name("Revenue");

        env.execute("Checkout");
    }

    static Order order(long i) {
        Order o = new Order();
        o.id = i;
        o.customer = "c" + (i % 500);
        o.sku = "sku-" + (i % 1000);
        // One order in fifty arrives broken.
        o.amount = i % 50 == 0 ? -1 : 10 + i % 90;
        return o;
    }

    static Payment payment(long i) {
        Payment p = new Payment();
        p.orderId = i;
        p.amount = 10 + i % 90;
        return p;
    }

    /** Sends invalid orders to the side output. */
    static final class Validate extends ProcessFunction<Order, Order> {
        private final OutputTag<Order> invalid;

        Validate(OutputTag<Order> invalid) {
            this.invalid = invalid;
        }

        @Override
        public void processElement(Order o, Context ctx, Collector<Order> out) {
            if (o.amount < 0) {
                ctx.output(invalid, o);
            } else {
                out.collect(o);
            }
        }
    }

    /** Emits an order once its payment has arrived; a side that waits more than a minute is dropped. */
    static final class MatchPayments extends KeyedCoProcessFunction<Long, Order, Payment, Order> {
        private transient ValueState<Order> order;
        private transient ValueState<Boolean> paid;

        @Override
        public void open(OpenContext ctx) {
            order = getRuntimeContext().getState(new ValueStateDescriptor<>("order", Order.class));
            paid = getRuntimeContext().getState(new ValueStateDescriptor<>("paid", Boolean.class));
        }

        @Override
        public void processElement1(Order o, Context ctx, Collector<Order> out) throws Exception {
            if (Boolean.TRUE.equals(paid.value())) {
                paid.clear();
                out.collect(o);
                return;
            }
            order.update(o);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 60_000);
        }

        @Override
        public void processElement2(Payment p, Context ctx, Collector<Order> out) throws Exception {
            Order o = order.value();
            if (o != null) {
                order.clear();
                out.collect(o);
                return;
            }
            paid.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 60_000);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<Order> out) {
            order.clear();
            paid.clear();
        }
    }

    /** Asks the inventory service for the stock of the order's SKU. */
    static final class EnrichFromInventory extends RichAsyncFunction<Order, Order> {
        private final String base;
        private transient HttpClient http;

        EnrichFromInventory(String base) {
            this.base = base;
        }

        @Override
        public void open(OpenContext ctx) {
            // HTTP/1.1: the default would first try to upgrade every plain connection to HTTP/2.
            http = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();
        }

        @Override
        public void asyncInvoke(Order o, ResultFuture<Order> result) {
            HttpRequest req = HttpRequest.newBuilder(URI.create(base + "/sku/" + o.sku)).GET().build();
            http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).whenComplete((res, err) -> {
                if (err != null) {
                    result.completeExceptionally(new IOException("inventory request for " + o.sku + " failed", err));
                } else if (res.statusCode() != 200) {
                    result.completeExceptionally(
                            new IOException("inventory answered " + res.statusCode() + " for " + o.sku + ": " + res.body()));
                } else {
                    o.stock = Integer.parseInt(res.body().trim());
                    result.complete(List.of(o));
                }
            });
        }
    }
}
