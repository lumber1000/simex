package com.github.lumber1000.simex.common

enum class OrderType {
    BUY_LIMIT,
    SELL_LIMIT
}

class Order(
    var id: Long,
    val type: OrderType,
    val ticker: String,
    val price: Int,
    var size: Int,
    val timestamp: Long
) {
    override fun toString() = "Order(id=$id, type=$type, ticker='$ticker', price=$price, size=$size, timestamp=$timestamp)"
}

abstract class MarketEvent(
    val id: Long,
    val ticker: String,
    val timestamp: Long
)

class Trade(
    id: Long,
    ticker: String,
    val price: Int,
    val size: Int,
    timestamp: Long
) : MarketEvent(id, ticker, timestamp) {
    override fun toString() = "Trade(id=$id, ticker='$ticker', price=$price, size=$size, timestamp=$timestamp)"
}

class NewOrderAdded(val order: Order) : MarketEvent(order.id, order.ticker, order.timestamp) {
    override fun toString() = "Order added to the book: $order"
}

class OrderRemoved(id: Long, ticker: String, timestamp: Long) : MarketEvent(id, ticker, timestamp) {
    override fun toString() = "Order removed from the book (id = $id, ticker=$ticker, timestamp=$timestamp)"
}
class OrderSizeChanged(id: Long, ticker: String, timestamp: Long, val newSize: Int) : MarketEvent(id, ticker, timestamp) {
    override fun toString() = "Order size changed (id = $id, ticker=$ticker, timestamp=$timestamp, newSize=$newSize)"
}

fun SimexServiceOuterClass.Order.toOrder() =
    Order(id, OrderType.values()[this.type.ordinal], ticker, price, size, timestamp)

fun Order.toMessage() = SimexServiceOuterClass.Order.newBuilder()
    .setId(id)
    .setType(SimexServiceOuterClass.Order.OrderType.forNumber(type.ordinal))
    .setTicker(ticker)
    .setPrice(price)
    .setSize(size)
    .setTimestamp(timestamp)
    .build()!!

fun SimexServiceOuterClass.MarketEvent.toMarketEvent() = when {
    hasTrade() -> Trade(id, ticker, trade.price, trade.size, timestamp)
    hasOrderAddedEvent() -> NewOrderAdded(orderAddedEvent.order.toOrder())
    hasOrderSizeChanged() -> OrderSizeChanged(id, ticker, timestamp, orderSizeChanged.newSize)
    else -> OrderRemoved(id, ticker, timestamp)
}

fun MarketEvent.toMessage(): SimexServiceOuterClass.MarketEvent {
    val eventBuilder = SimexServiceOuterClass.MarketEvent.newBuilder()
        .setId(id)
        .setTicker(ticker)
        .setTimestamp(timestamp)

    when (this) {
        is Trade -> eventBuilder.tradeBuilder.setPrice(price).setSize(size)
        is NewOrderAdded -> eventBuilder.orderAddedEventBuilder.setOrder(order.toMessage())
        is OrderSizeChanged -> eventBuilder.orderSizeChangedBuilder.setNewSize(newSize)
        is OrderRemoved -> { /* none */ }
        else -> error("unknown event")
    }

    return eventBuilder.build()
}