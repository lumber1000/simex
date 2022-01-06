package com.github.lumber1000.simex.common

import org.slf4j.Logger

fun setDefaultExceptionHandler(logger: Logger) {
    Thread.setDefaultUncaughtExceptionHandler { thread, exception ->
        logger.error("Uncaught exception in $thread: ", exception)
    }
}