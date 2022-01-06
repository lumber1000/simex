package com.github.lumber1000.simex.service

import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentHashMap
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import com.github.lumber1000.simex.common.*
import io.grpc.Server
import io.reactivex.rxjava3.schedulers.Schedulers
import java.util.concurrent.Executor

class SimexService(private val orderMatcher: OrderMatcher, private val ioExecutor: Executor) :
    SimexServiceGrpc.SimexServiceImplBase() {
    private val streamObservers: MutableMap<String, MutableMap<StreamObserver<SimexServiceOuterClass.MarketEvent>, String>> =
        ConcurrentHashMap()

    init {
        orderMatcher.events
            .observeOn(Schedulers.from(ioExecutor))
            .doOnNext { LOGGER.info("Event streamed: $it)") }
            .map { it.toMessage() }
            .subscribe { streamObservers[it.ticker]?.keys?.forEach { streamObserver -> streamObserver.onNext(it) } }
    }

    override fun submitOrder(
        request: SimexServiceOuterClass.Order,
        responseObserver: StreamObserver<SimexServiceOuterClass.SubmitOrderResponse>
    ) {
        val order = Order(0, OrderType.values()[request.type.ordinal], request.ticker, request.price, request.size, 0)
        LOGGER.info("Order received: $order")

        orderMatcher.enqueueNewOrderRequest(order) { id: Long ->
            ioExecutor.execute {
                responseObserver.onNext(SimexServiceOuterClass.SubmitOrderResponse.newBuilder().setOrderId(id).build())
                responseObserver.onCompleted()
            }
        }
    }

    override fun cancelOrder(
        request: SimexServiceOuterClass.CancelOrderRequest,
        responseObserver: StreamObserver<SimexServiceOuterClass.CancelOrderResponse>
    ) {
        val orderId = request.orderId
        LOGGER.info("Cancel request received: id = $orderId")

        orderMatcher.enqueueCancelRequest(orderId) { succeed: Boolean ->
            ioExecutor.execute {
                LOGGER.info(if (succeed) "Order cancelled: id = $orderId" else "Failed to cancel order: id = $orderId")
                responseObserver.onNext(
                    SimexServiceOuterClass.CancelOrderResponse.newBuilder()
                        .setSucceed(succeed)
                        .setErrorMessage(if (succeed) "" else "Order not found")
                        .build()
                )
                responseObserver.onCompleted()
            }
        }
    }

    override fun getMarketDataStream(responseObserver: StreamObserver<SimexServiceOuterClass.MarketEvent>):
            StreamObserver<SimexServiceOuterClass.MarketDataRequest> {

        fun unsubscribeResponseObserver() = streamObservers.values.forEach { it.remove(responseObserver) }

        return object : StreamObserver<SimexServiceOuterClass.MarketDataRequest> {
            override fun onNext(request: SimexServiceOuterClass.MarketDataRequest) {
                val tickersList = request.tickerList.toList()
                LOGGER.info("Market data streaming requested for $tickersList")

                // resubscribe client for new tickers list
                unsubscribeResponseObserver()
                tickersList.forEach {
                    val observersList = streamObservers.computeIfAbsent(it) { ConcurrentHashMap() }
                    observersList[responseObserver] = "" // using Map as Set
                }
            }

            override fun onError(t: Throwable?) = onCompleted()
            override fun onCompleted() {
                unsubscribeResponseObserver()
                responseObserver.onCompleted()
                LOGGER.info("Market data streaming cancelled in onCompleted.")
            }
        }
    }

    override fun getBook(
        request: SimexServiceOuterClass.BookRequest,
        responseObserver: StreamObserver<SimexServiceOuterClass.BookResponse>
    ) {
        val tickersList: List<String> = request.tickerList.toList()
        LOGGER.info("Order book requested for $tickersList")

        orderMatcher.enqueueGetBookRequest(tickersList) { ordersList ->
            ioExecutor.execute {
                val responseBuilder = SimexServiceOuterClass.BookResponse.newBuilder()
                ordersList.forEach { responseBuilder.addOrders(it.toMessage()) }
                responseObserver.onNext(responseBuilder.build())
                responseObserver.onCompleted()
                LOGGER.info(
                    "Order book sent with ids: ${
                        ordersList.asSequence().map { it.id }.joinToString(prefix = "[", postfix = "]")
                    }"
                )
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        @JvmStatic
        fun main(vararg args: String) {
            setDefaultExceptionHandler(LOGGER)
            val config = loadConfig()

            val orderMatcher = OrderMatcher()
            val ioExecutor = Executors.newCachedThreadPool()
            val simexService = SimexService(orderMatcher, ioExecutor)

            val server: Server = ServerBuilder.forPort(config.port)
                .executor(ioExecutor)
                .addService(simexService)
                .build()
                .start()

            Runtime.getRuntime().addShutdownHook(Thread {
                LOGGER.info("Shutting down server.")

                orderMatcher.stop()
                if (!orderMatcher.awaitTermination(3_000)) {
                    LOGGER.error("Failed to stop order matcher.")
                }

                server.shutdown()
            })

            server.awaitTermination()
        }
    }
}