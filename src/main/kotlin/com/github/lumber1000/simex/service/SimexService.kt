package com.github.lumber1000.simex.service

import java.util.logging.Logger
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentHashMap
import io.grpc.ServerBuilder
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import com.github.lumber1000.simex.common.*

fun main() {
    val orderMatcher = OrderMatcher()
    val simexService = SimexService(orderMatcher, LOGGER)

    val server = ServerBuilder.forPort(PORT)
        .executor(Executors.newSingleThreadExecutor())
        .addService(simexService)
        .build()
        .start()

    LOGGER.info("Service started on port $PORT")

    Runtime.getRuntime().addShutdownHook(Thread { server.shutdown() })
    server.awaitTermination()
}

class SimexService(private val orderMatcher: OrderMatcher, private val logger: Logger) : SimexServiceGrpc.SimexServiceImplBase() {
    private val streamObservers: MutableMap<String, MutableList<StreamObserver<SimexServiceOuterClass.MarketEvent>>> = ConcurrentHashMap()

    init {
        orderMatcher.events
            .doOnNext { logger.info("Event streamed: $it)") }
            .map { it.toMessage() }
            .subscribe { streamObservers[it.ticker]?.forEach { streamObserver -> streamObserver.onNext(it) } }
    }

    private var prevOrderId = 0L
    override fun submitOrder(
        request: SimexServiceOuterClass.Order,
        responseObserver: StreamObserver<SimexServiceOuterClass.SubmitOrderResponse>
    ) {
        val id = ++prevOrderId
        val order = Order(id, OrderType.values()[request.type.ordinal], request.ticker, request.price, request.size, System.currentTimeMillis())
        logger.info("Order received: $order")
        orderMatcher.matchOrder(order)
        val response = SimexServiceOuterClass.SubmitOrderResponse.newBuilder().setOrderId(id).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun cancelOrder(
        request: SimexServiceOuterClass.Order,
        responseObserver: StreamObserver<SimexServiceOuterClass.CancelOrderResponse>
    ) {
        val order = request.toOrder()
        logger.info("Cancel request received: $order")
        val succeed = orderMatcher.cancelOrder(order)
        logger.info(if (succeed) "Order cancelled: $order" else "Failed to cancel order: $order")
        responseObserver.onNext(SimexServiceOuterClass.CancelOrderResponse.newBuilder()
            .setSucceed(succeed)
            .setErrorMessage(if (succeed) "" else "Order not found")
            .build())
        responseObserver.onCompleted()
    }

    override fun getMarketDataStream(
        request: SimexServiceOuterClass.MarketDataRequest,
        responseObserver: StreamObserver<SimexServiceOuterClass.MarketEvent>
    ) {
        val tickersList: List<String> = request.tickerList.toList()
        logger.info("Market data streaming requested for $tickersList")
        tickersList.forEach {
            val observersList = streamObservers.computeIfAbsent(it) { ArrayList() }
            observersList.add(responseObserver)
        }

        (responseObserver as ServerCallStreamObserver<SimexServiceOuterClass.MarketEvent>)
            .setOnCancelHandler {
                tickersList.forEach { streamObservers[it]!!.remove(responseObserver) }
                logger.info("Market data streaming cancelled for $tickersList")
            }
    }

    override fun getBook(
        request: SimexServiceOuterClass.BookRequest,
        responseObserver: StreamObserver<SimexServiceOuterClass.BookResponse>
    ) {
        val tickersList: List<String> = request.tickerList.toList()
        logger.info("Order book requested for $tickersList")

        val ordersList = orderMatcher.getBookForTickers(tickersList)
        val responseBuilder = SimexServiceOuterClass.BookResponse.newBuilder()

        ordersList.forEach { responseBuilder.addOrders(it.toMessage()) }
        responseObserver.onNext(responseBuilder.build())
        responseObserver.onCompleted()
        logger.info("Order book sent with ids: ${ordersList.asSequence().map { it.id }.joinToString(prefix = "[", postfix = "]")}")
    }
}