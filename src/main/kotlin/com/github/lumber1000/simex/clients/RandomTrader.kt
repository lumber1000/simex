package com.github.lumber1000.simex.clients

import kotlin.random.Random
import mu.KotlinLogging
import com.github.lumber1000.simex.common.*
import kotlinx.coroutines.*
import java.util.concurrent.*

class RandomTrader(
    host: String,
    port: Int,
    private val ticker: String,
    private val orderType: OrderType,
    private val random: Random,
    private var numberOfOrders: Int
) : BaseClient(host, port) {

    private suspend fun placeOrders() {
        // waitForConnection()

        repeat(numberOfOrders) {
            val order = Order(0L, orderType, ticker, random.nextInt(100, 200), random.nextInt(1, 100), 0L)
            stubRxAdapter.submitOrder(order)
            LOGGER.info("Send order: $order")
            delay(random.nextLong(200, 800))
        }
    }

    private suspend fun cancelUnsatisfiedOrders() {
        var book = stubRxAdapter.getBook(listOf(ticker)).blockingGet()
        if (book.isEmpty()) return

        println("\nOrderBook for $ticker (${book.size} orders):\n\n$book\n")
        println("Cancel unsatisfied orders:\n")

        book.forEach {
            LOGGER.info("Cancel order: $it")
            stubRxAdapter.cancelOrder(it)
        }

        delay(10)

        book = stubRxAdapter.getBook(listOf(ticker)).blockingGet()
        println("\nOrderBook for $ticker (${book.size} orders)\n\n$book\n")
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        @JvmStatic
        fun main(vararg args: String) {
            setDefaultExceptionHandler(LOGGER)
            val config = loadConfig()
            val numberOfOrders = 20

            fun createTrader(ticker: String, type: OrderType, seed: Int) =
                RandomTrader(config.hostname, config.port, ticker, type, Random(seed), numberOfOrders)

            val traders = listOf(
                createTrader("T1", OrderType.BUY_LIMIT, 1000),
                createTrader("T1", OrderType.SELL_LIMIT, 2000),
                createTrader("T2", OrderType.BUY_LIMIT, 3000),
                createTrader("T2", OrderType.SELL_LIMIT, 4000)
            )

            Executors.newSingleThreadExecutor().asCoroutineDispatcher().use { context ->
                runBlocking {
                    traders.first().waitForConnection()

                    traders.map { launch(context) { it.placeOrders() } }
                        .toList()
                        .joinAll()

                    traders.forEach { it.cancelUnsatisfiedOrders() }
                }
            }
        }
    }
}