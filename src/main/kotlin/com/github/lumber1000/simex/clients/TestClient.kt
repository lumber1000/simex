package com.github.lumber1000.simex.clients

import java.util.logging.Logger
import com.github.lumber1000.simex.common.*

fun main() {
    val client = TestClient(HOSTNAME, PORT, LOGGER)
    try {
        client.placeOrders()
    } finally {
        client.shutdown()
    }
}

class TestClient(hostname: String, port: Int, logger: Logger) : BaseClient(hostname, port, logger) {
    fun placeOrders() {
        waitForConnection()

        // TODO: cancel streaming
        stubRxAdapter.requestMarketDataStream(listOf("T1", "T2"))
            .subscribe({ println("EVENT: $it") }, Throwable::printStackTrace)

        val order1 = Order(0, OrderType.BUY_LIMIT, "T1", 100, 24, 0)
        stubRxAdapter.submitOrder(order1)
            .blockingSubscribe({ id: Long ->
                order1.id = id
                logger.info("Order accepted: $order1")
            }, Throwable::printStackTrace
        )

        val order2 = Order(0, OrderType.SELL_LIMIT, "T1", 100, 16, 0)
        stubRxAdapter.submitOrder(order2)
            .blockingSubscribe(
                { id: Long ->
                    order2.id = id
                    logger.info("Order accepted: $order2")
                }, Throwable::printStackTrace)

        stubRxAdapter.cancelOrder(order1).blockingSubscribe(
            {
                if (it.succeed) {
                    logger.info("Order successfully cancelled: $order1")
                } else {
                    logger.info("Failed to cancel order: $order1, error: ${it.errorMessage}")
                }
            }, Throwable::printStackTrace
        )

        val order3 = Order(0, OrderType.BUY_LIMIT, "T1", 100, 24, 0)
        stubRxAdapter.submitOrder(order3)
            .blockingSubscribe({ id: Long ->
                order3.id = id
                logger.info("Order accepted: $order3")
            }, Throwable::printStackTrace)

        stubRxAdapter.cancelOrder(order3).blockingSubscribe(
            {
                if (it.succeed) {
                    logger.info("Order successfully cancelled: $order3")
                } else {
                    logger.info("Failed to cancel order: $order3, error: ${it.errorMessage}")
                }
            }, Throwable::printStackTrace
        )

        val order4 = Order(0, OrderType.SELL_LIMIT, "T1", 100, 24, 0)
        stubRxAdapter.submitOrder(order4)
            .blockingSubscribe({ id: Long ->
                order4.id = id
                logger.info("New order accepted: $order4")
            }, Throwable::printStackTrace)

        stubRxAdapter.getBook(listOf("T1", "T2"))
            .blockingSubscribe({ println("Book: $it") }, Throwable::printStackTrace)
    }
}