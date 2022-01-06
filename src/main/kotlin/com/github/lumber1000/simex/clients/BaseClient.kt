package com.github.lumber1000.simex.clients

import com.github.lumber1000.simex.common.SimexServiceGrpc
import io.grpc.ConnectivityState
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

abstract class BaseClient(private val hostname: String, private val port: Int) {
    private val channel = ManagedChannelBuilder
        .forAddress(hostname, port)
        .usePlaintext()
        .build()

    protected val stubRxAdapter = StubRxAdapter(SimexServiceGrpc.newStub(channel).withWaitForReady())

    protected suspend fun waitForConnection() {
        var delayMs = 1_000L
        while (channel.getState(true) != ConnectivityState.READY) {
            LOGGER.info("Connecting to $hostname:$port...")
            delay(delayMs)
            if (delayMs < 5000L) delayMs += 1_000L
        }
        LOGGER.info("Connected.")
    }

    protected fun waitForConnectionBlocking() = runBlocking { waitForConnection() }

    protected fun shutdown() {
        val isTerminated = channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)

        if (isTerminated) {
            LOGGER.info("gRPC channel successfully terminated")
        } else {
            LOGGER.error("Failed to shutdown gRPC channel")
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}