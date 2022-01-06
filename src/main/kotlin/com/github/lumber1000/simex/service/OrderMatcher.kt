package com.github.lumber1000.simex.service

import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.Comparator
import kotlin.collections.HashMap
import kotlin.math.min
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.subjects.PublishSubject
import com.github.lumber1000.simex.common.*
import kotlin.NoSuchElementException
import kotlin.concurrent.thread

class OrderMatcher {
    private val _events = PublishSubject.create<MarketEvent>()
    val events: Observable<MarketEvent> get() = _events

    private val orderBooks: MutableMap<String, TreeMap<PriceLevel, PriceLevelQueue>> = HashMap()
    private val orderEntries: MutableMap<Long, Entry> = HashMap()

    private var prevTradeId = 0L
    private var prevOrderId = 0L
    private val requestQueue: BlockingQueue<Request> = LinkedBlockingQueue()

    private val executorThread = thread {
        try {
            while (!Thread.currentThread().isInterrupted) {
                executeRequest(requestQueue.take())
            }
        } catch (_: InterruptedException) { /* exit */ }
    }

    fun stop() = executorThread.interrupt()
    fun awaitTermination(timeoutMillis: Long): Boolean {
        executorThread.join(timeoutMillis)
        return !executorThread.isAlive
    }

    fun enqueueNewOrderRequest(order: Order, newOrderResponseHandler: ((id: Long) -> Unit)) {
        val request = Request(
            Request.Type.NEW_ORDER,
            ResponseHandler(newOrderResponseHandler),
            order
        )
        requestQueue.offer(request)
    }

    fun enqueueCancelRequest(id: Long, cancelResponseHandler: ((succeed: Boolean) -> Unit)) {
        val request = Request(
            Request.Type.CANCEL_ORDER,
            ResponseHandler(cancelResponseHandler = cancelResponseHandler),
            orderId = id
        )
        requestQueue.offer(request)
    }

    fun enqueueGetBookRequest(tickersList: List<String>, bookResponseHandler: ((List<Order>) -> Unit)) {
        val request = Request(
            Request.Type.BOOK_REQ,
            ResponseHandler(bookResponseHandler = bookResponseHandler),
            tickers = tickersList
        )
        requestQueue.offer(request)
    }

    private fun executeRequest(request: Request) {
        when (request.type) {
            Request.Type.NEW_ORDER -> {
                val order = request.order!!
                order.id = ++prevOrderId
                matchOrder(order)
                request.responseHandler.newOrderResponseHandler!!(order.id)
            }
            Request.Type.CANCEL_ORDER -> {
                val id = request.orderId
                val res = cancelOrder(id)
                request.responseHandler.cancelResponseHandler!!(res)
            }
            Request.Type.BOOK_REQ -> {
                val tickers = request.tickers!!
                val res = getBookForTickers(tickers)
                request.responseHandler.bookResponseHandler!!(res)
            }
        }
    }

    private class ResponseHandler (
        val newOrderResponseHandler: ((id: Long) -> Unit)? = null,
        val cancelResponseHandler: ((succeed: Boolean) -> Unit)? = null,
        val bookResponseHandler: ((List<Order>) -> Unit)? = null
    )

    private class Request (
        val type: Type,
        val responseHandler: ResponseHandler,
        val order: Order? = null,
        val orderId: Long = 0,
        val tickers: List<String>? = null
    ) {
        enum class Type {
            NEW_ORDER,
            CANCEL_ORDER,
            BOOK_REQ
        }
    }

    private fun matchOrder(order: Order) {
        val ordersBook = orderBooks.computeIfAbsent(order.ticker) {
            TreeMap<PriceLevel, PriceLevelQueue>(Comparator.comparing(PriceLevel::type).reversed().thenComparing(PriceLevel::price))
        }
        val orderPriceLevel = PriceLevel(order.price, order.type)

        val oppositeOrdersIterator: MutableIterator<MutableMap.MutableEntry<PriceLevel, PriceLevelQueue>> = when (order.type) {
            OrderType.BUY_LIMIT -> ordersBook.iterator()
            OrderType.SELL_LIMIT -> ordersBook.descendingMap().iterator()
        }

        while (order.size > 0 && oppositeOrdersIterator.hasNext()) {
            val (priceLevel, bestPriceQueue) = oppositeOrdersIterator.next()
            if (!orderPriceLevel.isMatchingWith(priceLevel)) break

            while (order.size > 0) {
                check(!bestPriceQueue.isEmpty()) { "Empty orders queue" }

                val bestOppositeOrder = bestPriceQueue.element()
                val tradeSize = min(order.size, bestOppositeOrder.size)
                _events.onNext(Trade(++prevTradeId, order.ticker, bestOppositeOrder.price, tradeSize, System.currentTimeMillis()))
                order.size -= tradeSize

                if (bestOppositeOrder.size == tradeSize) {
                    orderEntries.remove(bestOppositeOrder.id)
                    bestPriceQueue.remove()
                    _events.onNext(OrderRemoved(bestOppositeOrder.id, bestOppositeOrder.ticker, System.currentTimeMillis()))

                    if (bestPriceQueue.isEmpty()) {
                        oppositeOrdersIterator.remove()
                        break
                    }
                } else {
                    bestOppositeOrder.size -= tradeSize
                    _events.onNext(OrderSizeChanged(bestOppositeOrder.id, bestOppositeOrder.ticker, System.currentTimeMillis(), bestOppositeOrder.size))
                }
            }
        }

        if (order.size > 0) {
            val priceLevelQueue = ordersBook.computeIfAbsent(orderPriceLevel) { PriceLevelQueue(ordersBook, orderPriceLevel) }
            val newEntry = priceLevelQueue.add(order)
            orderEntries[order.id] = newEntry
            _events.onNext(NewOrderAdded(order))
        }
    }

    private fun cancelOrder(id: Long): Boolean {
        val entry = orderEntries.remove(id) ?: return false
        entry.selfRemove()
        _events.onNext(OrderRemoved(id, entry.order.ticker, System.currentTimeMillis()))
        return true
    }

    private fun getBookForTickers(tickers: List<String>): List<Order> = tickers.asSequence()
        .flatMap { ticker ->
            orderBooks[ticker]
                ?.asSequence()
                ?.flatMap { it.value.asSequence() }
                ?: emptySequence()
        }
        .toList()

    private class PriceLevel(
        val price: Int,
        val type: OrderType
    ) {
        fun isMatchingWith(otherPriceLevel: PriceLevel) =
            (type != otherPriceLevel.type) && when (type) {
                OrderType.BUY_LIMIT -> price >= otherPriceLevel.price
                OrderType.SELL_LIMIT -> price <= otherPriceLevel.price
            }
    }

    private class PriceLevelQueue(
        private val parentTree: MutableMap<PriceLevel, PriceLevelQueue>,
        private val priceLevel: PriceLevel
    ) : Iterable<Order> {
        private val dummyHead = Entry.getDummyEntry()
        private val dummyTail = Entry.getDummyEntry()
        init {
            dummyHead.next = dummyTail
            dummyTail.prev = dummyHead
        }

        private var size: Int = 0
        fun isEmpty() = size == 0

        private fun onElementRemovedListener() {
            size--
            if (isEmpty()) {
                check(dummyHead.next === dummyTail && dummyTail.prev === dummyHead)
                parentTree.remove(priceLevel)
            }
        }

        fun add(element: Order): Entry {
            val newEntry = Entry(element, dummyTail.prev, dummyTail, this::onElementRemovedListener)

            dummyTail.prev.next = newEntry
            dummyTail.prev = newEntry
            size++

            return newEntry
        }

        fun element(): Order {
            if (isEmpty()) throw NoSuchElementException("Queue is empty")
            return dummyHead.next.order
        }

        fun remove(): Order {
            if (isEmpty()) throw NoSuchElementException("Queue is empty")
            val entry = dummyHead.next

            dummyHead.next = entry.next
            entry.next.prev = dummyHead
            size--

            return entry.order
        }

        override fun iterator(): Iterator<Order> = object : Iterator<Order> {
            private var current = dummyHead
            override fun hasNext() = current.next != dummyTail
            override fun next(): Order {
                if (!hasNext()) throw NoSuchElementException()
                current = current.next
                return current.order
            }
        }
    }

    private class Entry {
        private val _order: Order?
        private var _prev: Entry?
        private var _next: Entry?
        private val onElementRemovedListener: () -> Unit

        val order: Order get() = getNonNull(_order)
        var prev: Entry get() = getNonNull(_prev)
            set(value) { _prev = value}
        var next: Entry get() = getNonNull(_next)
            set(value) { _next = value}

        constructor(order: Order, prev: Entry, next: Entry, listener: () -> Unit) {
            _order = order
            _prev = prev
            _next = next
            onElementRemovedListener = listener
        }

        // for dummy entries only
        private constructor() {
            _order = null
            _prev = null
            _next = null
            onElementRemovedListener = { error("Dummy entry removal is restricted") }
        }

        fun selfRemove() {
            prev._next = _next
            next._prev = _prev
            onElementRemovedListener.invoke()
        }

        // Nulls are only possible in dummy entries.
        // Dummy entries does not contain queue elements and should not ever be read
        private fun <T> getNonNull(value: T?): T = value ?: error("Dummy entry should not be read")

        companion object {
            fun getDummyEntry() = Entry()
        }
    }
}