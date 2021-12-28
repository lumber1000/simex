package com.github.lumber1000.simex.service

import java.util.*
import kotlin.Comparator
import kotlin.collections.HashMap
import kotlin.math.min
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.subjects.PublishSubject
import com.github.lumber1000.simex.common.*

class OrderMatcher {
    private val eventPublisher = PublishSubject.create<MarketEvent>()
    val events: Observable<MarketEvent> get() = eventPublisher

    private var prevTradeId = 0L
    private val orderBooks = HashMap<String, Book>()

    private class Book {
        private val orderSets = arrayOf(
            TreeSet(Comparator.comparing(Order::price).reversed().thenComparing(Order::id)),
            TreeSet(Comparator.comparing(Order::price).thenComparingLong(Order::id))
        )

        operator fun get(type: OrderType): NavigableSet<Order> = orderSets[type.ordinal]
    }

    fun matchOrder(order: Order) {
        val ordersBook = orderBooks.computeIfAbsent(order.ticker) { Book() }

        val oppositeOrdersIterator: MutableIterator<Order> = ordersBook[order.type.opposite].iterator()
        while (order.size > 0 && oppositeOrdersIterator.hasNext()) {
            val bestOppositeOrder = oppositeOrdersIterator.next()
            if (!order.isAcceptablePrice(bestOppositeOrder)) break

            val tradeSize = min(order.size, bestOppositeOrder.size)
            eventPublisher.onNext(Trade(++prevTradeId, order.ticker, bestOppositeOrder.price, tradeSize, System.currentTimeMillis()))
            order.size -= tradeSize

            if (bestOppositeOrder.size == tradeSize) {
                oppositeOrdersIterator.remove()
                eventPublisher.onNext(OrderRemoved(bestOppositeOrder.id, order.ticker, System.currentTimeMillis()))
            } else {
                bestOppositeOrder.size -= tradeSize
                eventPublisher.onNext(OrderSizeChanged(bestOppositeOrder.id, order.ticker, System.currentTimeMillis(), bestOppositeOrder.size))
            }
        }

        if (order.size > 0) {
            val isAdded = ordersBook[order.type].add(order)
            check(isAdded)
            eventPublisher.onNext(NewOrderAdded(order))
        }
    }

    private fun listForOrder(order: Order): SortedSet<Order> {
        val ordersBook = orderBooks.computeIfAbsent(order.ticker) { Book() }
        return ordersBook[order.type]
    }

    fun cancelOrder(order: Order): Boolean {
        val res = listForOrder(order).remove(order)
        if (res) eventPublisher.onNext(OrderRemoved(order.id, order.ticker, System.currentTimeMillis()))
        return res
    }

    fun getBookForTickers(tickers: List<String>) = tickers.asSequence()
        .flatMap { ticker ->
            OrderType.values().flatMap { type ->
                orderBooks[ticker]?.get(type)?.asSequence().orEmpty()
            }
        }.toList()
}