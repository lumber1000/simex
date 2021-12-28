package com.github.lumber1000.simex.clients

import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import io.grpc.ConnectivityState
import io.grpc.ManagedChannelBuilder
import com.github.lumber1000.simex.common.SimexServiceGrpc

abstract class BaseClient(private val hostname: String, private val port: Int, protected val logger: Logger) {
    private val channel = ManagedChannelBuilder
        .forAddress(hostname, port)
        .usePlaintext()
        .build()

    protected val stubRxAdapter = StubRxAdapter(SimexServiceGrpc.newStub(channel))

    fun shutdown() = channel
        .shutdown()
        .awaitTermination(5, TimeUnit.SECONDS)

    protected fun waitForConnection() {
        while (channel.getState(true) != ConnectivityState.READY) {
            logger.info("Connecting to $hostname:$port...")
            Thread.sleep(3_000)
        }
        logger.info("Connected.")
    }
}