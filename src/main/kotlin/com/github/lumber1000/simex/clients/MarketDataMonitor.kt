package com.github.lumber1000.simex.clients

import mu.KotlinLogging
import com.github.lumber1000.simex.common.loadConfig
import com.github.lumber1000.simex.common.setDefaultExceptionHandler

class MarketDataMonitor(
    hostname: String,
    port: Int,
    private val tickers: List<String>
) : BaseClient(hostname, port) {
    fun run() {
        waitForConnectionBlocking()

        stubRxAdapter.requestMarketDataStream(tickers).blockingSubscribe(
            { LOGGER.info("Market data event: $it") },
            { LOGGER.error("Market data exception: ", it) },
            { LOGGER.info("Market data onComplete.") },
        )
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        @JvmStatic
        fun main(vararg args: String) {
            setDefaultExceptionHandler(LOGGER)
            val config = loadConfig()

            val monitor = MarketDataMonitor(config.hostname, config.port, listOf("T1", "T2"))

            try {
                monitor.run()
            } finally {
                monitor.shutdown()
            }
        }
    }
}