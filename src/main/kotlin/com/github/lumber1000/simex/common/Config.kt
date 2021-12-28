package com.github.lumber1000.simex.common

import java.util.logging.Logger
import java.util.logging.LogManager
import com.github.lumber1000.simex.service.SimexService

const val HOSTNAME = "localhost"
const val PORT = 50051

val LOGGER: Logger = run {
    val stream = SimexService::class.java.classLoader.getResourceAsStream("logging.properties")!!
    LogManager.getLogManager().readConfiguration(stream)
    Logger.getLogger(SimexService::class.java.name)
}