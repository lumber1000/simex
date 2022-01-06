package com.github.lumber1000.simex.common

import mu.KotlinLogging
import java.io.FileInputStream
import java.io.IOException
import java.util.*

class Config (
    val hostname: String,
    val port: Int
)

fun loadConfig(): Config {
    val logger = KotlinLogging.logger {}

    fun <T> useDefaultValue(valueName: String, value:  T): T {
        logger.error { "ERROR Could not find value for key $valueName, using default value: $value" }
        return value
    }

    val keyHostname = "HOSTNAME"
    val keyPort = "PORT"
    val defaultHostname = "localhost"
    val defaultPort = 50051

    val properties = Properties()
    try {
        FileInputStream("config.properties").use {
            properties.load(it)
        }
    } catch (e: IOException) {
        logger.error("Failed to read configuration file.")
    }

    val hostname = properties[keyHostname] as? String ?: run {
        useDefaultValue(keyHostname, defaultHostname)
    }

    val portString = (properties[keyPort] as? String)

    val port = try {
        portString?.toInt()
    } catch (e: NumberFormatException) {
        null
    } ?: run {
        useDefaultValue(keyPort, defaultPort)
    }

    return Config(hostname, port)
}