package com.github.lumber1000.simex.clients

import java.util.logging.Logger
import com.github.lumber1000.simex.common.*

fun main() {
    val monitor = MarketDataMonitor(HOSTNAME, PORT, LOGGER, listOf("T1", "T2"))
    try {
        monitor.run()
    } finally {
        monitor.shutdown()
    }
}

class MarketDataMonitor(
    hostname: String,
    port: Int,
    logger: Logger,
    private val tickers: List<String>
) : BaseClient(hostname, port, logger) {
    fun run() {
        waitForConnection()
        stubRxAdapter.requestMarketDataStream(tickers).blockingSubscribe {
            println(it.toString())
        }
    }
}