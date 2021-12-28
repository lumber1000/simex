package com.github.lumber1000.simex.clients

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.random.Random
import com.github.lumber1000.simex.common.*

fun main() {
    val numberOfTraders = 4
    val executor = Executors.newFixedThreadPool(numberOfTraders)
    val barrier = CyclicBarrier(numberOfTraders)

    executor.submit(RandomTrader(HOSTNAME, PORT, LOGGER, barrier, "T1", OrderType.BUY_LIMIT, Random(1000)))
    executor.submit(RandomTrader(HOSTNAME, PORT, LOGGER, barrier, "T1", OrderType.SELL_LIMIT, Random(2000)))
    executor.submit(RandomTrader(HOSTNAME, PORT, LOGGER, barrier, "T2", OrderType.BUY_LIMIT, Random(3000)))
    executor.submit(RandomTrader(HOSTNAME, PORT, LOGGER, barrier, "T2", OrderType.SELL_LIMIT, Random(4000)))

    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.MINUTES)
}

class RandomTrader(
    host: String,
    port: Int,
    logger: Logger,
    private val barrier: CyclicBarrier,
    private val ticker: String,
    private val orderType: OrderType,
    private val random: Random
    ) : BaseClient(host, port, logger), Runnable {

    override fun run() {
        waitForConnection()

        repeat(20) {
            val order = Order(0L, orderType, ticker, random.nextInt(100, 200), random.nextInt(1, 100), 0L)
            stubRxAdapter.submitOrder(order)
            logger.info("Send order: $order")
            Thread.sleep(random.nextLong(200, 800))
        }

        barrier.await()

        if (orderType == OrderType.BUY_LIMIT) {
            var book = stubRxAdapter.getBook(listOf(ticker)).blockingGet()
            println("\nOrderBook for $ticker (${book.size} orders)\n\n$book\n")
            println("\n\nCancel all orders\n")

            book.forEach {
                logger.info("Cancel order: $it")
                stubRxAdapter.cancelOrder(it)
            }

            book = stubRxAdapter.getBook(listOf(ticker)).blockingGet()
            println("\n\nOrderBook for $ticker (${book.size} orders)\n$book\n")
        }
    }
}