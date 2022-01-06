package com.github.lumber1000.simex.clients

import com.github.lumber1000.simex.common.*
import mu.KotlinLogging

class TestClient(hostname: String, port: Int) : BaseClient(hostname, port) {

    private fun placeOrders() {
        waitForConnectionBlocking()

        val marketDataStream = stubRxAdapter.requestMarketDataStream(listOf("T1", "T2"))
        val marketDataSubscription = marketDataStream
            .subscribe({ println("EVENT: $it") }, Throwable::printStackTrace)

        val order1 = placeNewOrder(OrderType.BUY_LIMIT, "T1", 100, 24)
        val order2 = placeNewOrder(OrderType.SELL_LIMIT, "T1", 100, 16)
        cancelOrder(order1)
        val order3 = placeNewOrder(OrderType.BUY_LIMIT, "T1", 100, 24)
        cancelOrder(order3)
        val order4 = placeNewOrder(OrderType.SELL_LIMIT, "T2", 80, 24)

        stubRxAdapter.getBook(listOf("T1", "T2"))
            .blockingSubscribe({ println("Book: $it") }, Throwable::printStackTrace)

        cancelOrder(order2)
        cancelOrder(order4)
        cancelOrder(order2)

        marketDataSubscription.dispose() // stop streaming
    }

    private fun placeNewOrder(type: OrderType, ticker: String, price: Int, size: Int): Order {
        val order = Order(0, type, ticker, price, size, 0)
        stubRxAdapter.submitOrder(order)
            .blockingSubscribe({ id: Long ->
                order.id = id
                LOGGER.info("New order accepted: $order")
            }, Throwable::printStackTrace)

        return order
    }

    private fun cancelOrder(order: Order) {
        stubRxAdapter.cancelOrder(order).blockingSubscribe(
            {
                if (it.succeed) {
                    LOGGER.info("Order successfully cancelled: $order")
                } else {
                    LOGGER.info("Failed to cancel order: $order, error: ${it.errorMessage}")
                }
            }, Throwable::printStackTrace
        )
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        @JvmStatic
        fun main(vararg args: String) {
            setDefaultExceptionHandler(LOGGER)
            val config = loadConfig()

            val client = TestClient(config.hostname, config.port)
            try {
                client.placeOrders()
            } finally {
                client.shutdown()
            }
        }
    }
}