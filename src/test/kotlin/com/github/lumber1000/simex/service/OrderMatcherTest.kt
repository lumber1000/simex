package com.github.lumber1000.simex.service

import java.util.stream.Collectors
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.assertj.core.api.Assertions.assertThat
import com.github.lumber1000.simex.common.*

internal class OrderMatcherTest {
    private val matcher = OrderMatcher()

    private val tradesList = mutableListOf<Trade>()
    private val addedOrdersList = mutableListOf<Order>()
    private val removedOrdersIdsList = mutableListOf<Long>()
    private val changedOrdersList = mutableListOf<OrderSizeChanged>()

    @BeforeEach
    fun collectEvents() {
        matcher.events.subscribe {
            when (it) {
                is Trade -> tradesList.add(it)
                is NewOrderAdded -> addedOrdersList.add(it.order)
                is OrderRemoved -> removedOrdersIdsList.add(it.id)
                is OrderSizeChanged -> changedOrdersList.add(it)
                else -> throw IllegalStateException("unknown event")
            }
        }
    }

    private val order1 = Order(1L, OrderType.BUY_LIMIT, "T1", 100, 10, 0)
    private val order2 = Order(2L, OrderType.SELL_LIMIT, "T1", 100, 10, 0)
    private val order3 = Order(3L, OrderType.SELL_LIMIT, "T1", 100, 7, 0)
    private val order4 = Order(4L, OrderType.SELL_LIMIT, "T2", 100, 10, 0)
    private val order5 = Order(5L, OrderType.SELL_LIMIT, "T1", 90, 10, 0)

    @Test
    fun testAddSingleOrder() {
        matcher.matchOrder(order1)

        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1).contains(order1)
        assertThat(tradesList).isEmpty()
        assertThat(addedOrdersList).hasSize(1)
        assertThat(removedOrdersIdsList).isEmpty()
        assertThat(changedOrdersList).isEmpty()
    }

    @Test
    fun testSingleTradeSameSize() {
        matcher.matchOrder(order1)
        matcher.matchOrder(order2)

        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).isEmpty()
        assertThat(tradesList).hasSize(1)
        assertThat(addedOrdersList).hasSize(1).contains(order1)
        assertThat(removedOrdersIdsList).hasSize(1).contains(order1.id)
        assertThat(changedOrdersList).isEmpty()
    }

    @Test
    fun testSingleTradeDiffSize() {
        matcher.matchOrder(order1)
        matcher.matchOrder(order3)

        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1).contains(order1)
        assertThat(tradesList).hasSize(1)
        assertThat(tradesList[0].size).isEqualTo(7)
        assertThat(addedOrdersList).hasSize(1)
        assertThat(addedOrdersList[0].id).isEqualTo(1)
        assertThat(removedOrdersIdsList).isEmpty()
        assertThat(changedOrdersList).hasSize(1)
        assertThat(changedOrdersList[0].id).isEqualTo(order1.id)
        assertThat(changedOrdersList[0].newSize).isEqualTo(3)
    }

    @Test
    fun testSingleTradeDiffSize2() {
        matcher.matchOrder(order3)
        matcher.matchOrder(order1)
        val book = matcher.getBookForTickers(listOf(order1.ticker))

        assertThat(book).hasSize(1).contains(order1)
        assertThat(book[0].size).isEqualTo(3)
        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1).contains(order1)
        assertThat(tradesList).hasSize(1)
        assertThat(tradesList[0].size).isEqualTo(7)
        assertThat(addedOrdersList).hasSize(2).contains(order1, order3)
        assertThat(removedOrdersIdsList).hasSize(1).contains(order3.id)
        assertThat(changedOrdersList).isEmpty()
    }

    @Test
    fun testRemoveNonExistentOrder() {
        matcher.matchOrder(order1)
        assertThat(matcher.cancelOrder(order2)).isFalse()

        val book = matcher.getBookForTickers(listOf(order1.ticker))
        assertThat(book).hasSize(1).contains(order1)
        assertThat(tradesList).isEmpty()
        assertThat(removedOrdersIdsList).isEmpty()
    }

    @Test
    fun testRemoveExistentOrder() {
        matcher.matchOrder(order1)
        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1).contains(order1)
        assertThat(matcher.cancelOrder(order1)).isTrue()
        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).isEmpty()
        assertThat(removedOrdersIdsList).hasSize(1).contains(order1.id)

        matcher.matchOrder(order2)
        assertThat(tradesList).isEmpty()
        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1).contains(order2)
    }

    @Test
    fun testMatch() {
        matcher.matchOrder(order1)
        matcher.matchOrder(order2)
        assertThat(matcher.cancelOrder(order1)).isFalse()
        assertThat(matcher.cancelOrder(order2)).isFalse()

        assertThat(tradesList).hasSize(1)
        assertThat(tradesList[0]. size).isEqualTo(10)
    }

    @Test
    fun testMatchPartial() {
        order2.size = 5
        matcher.matchOrder(order2)
        matcher.matchOrder(order1)
        order2.size = 5
        matcher.matchOrder(order2)

        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).isEmpty()
        assertThat(tradesList).hasSize(2)
        assertThat(tradesList.sumOf { it.size }).isEqualTo(10)
        assertThat(addedOrdersList).hasSize(2).contains(order1, order2)
    }

    @Test
    fun testDifferentTickers() {
        matcher.matchOrder(order1)
        matcher.matchOrder(order4)

        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1).contains(order1)
        assertThat(matcher.getBookForTickers(listOf(order4.ticker))).hasSize(1).contains(order4)

        assertThat(matcher.cancelOrder(order1)).isTrue()
        assertThat(matcher.cancelOrder(order4)).isTrue()

        assertThat(matcher.getBookForTickers(listOf(order1.ticker, order4.ticker))).isEmpty()
        assertThat(tradesList).isEmpty()
        assertThat(removedOrdersIdsList).hasSize(2).contains(order1.id, order4.id)
    }

    @Test
    fun testBestPrice() {
        matcher.events.collect(Collectors.counting())
        matcher.matchOrder(order5)
        matcher.matchOrder(order1)
        assertThat(matcher.cancelOrder(order1)).isFalse()
        assertThat(matcher.cancelOrder(order5)).isFalse()
        assertThat(tradesList).hasSize(1)
        assertThat(tradesList[0].price).isEqualTo(90)
    }

    @Test
    fun testGetBookForTickers1() {
        matcher.matchOrder(order1)
        assertThat(matcher.getBookForTickers(listOf(order1.ticker, order4.ticker))).hasSize(1)
    }

    @Test
    fun testGetBookForTickers2() {
        matcher.matchOrder(order1)
        matcher.matchOrder(order4)

        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1)
        assertThat(matcher.getBookForTickers(listOf(order1.ticker, order4.ticker))).hasSize(2)
    }

    @Test
    fun testGetBook1() {
        matcher.matchOrder(order1)
        val res = matcher.getBookForTickers(listOf(order1.ticker))

        assertThat(res).hasSize(1).hasSameElementsAs(listOf(order1))
    }

    @Test
    fun testGetBook2() {
        matcher.matchOrder(order1)
        matcher.matchOrder(order4)

        assertThat(matcher.getBookForTickers(listOf(order1.ticker))).hasSize(1).hasSameElementsAs(listOf(order1))
        assertThat(matcher.getBookForTickers(listOf(order1.ticker, order4.ticker))).hasSize(2).hasSameElementsAs(listOf(order1, order4))
    }
}