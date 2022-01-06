package com.github.lumber1000.simex.clients

import kotlin.streams.toList
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.SingleSubject
import com.google.protobuf.GeneratedMessageV3
import io.grpc.stub.StreamObserver
import com.github.lumber1000.simex.common.*

class StubRxAdapter(private val stub: SimexServiceGrpc.SimexServiceStub) {

    private class SubmitOrderResponseObserver :
        BaseResponseObserver<SimexServiceOuterClass.SubmitOrderResponse, Long>() {
        override fun map(value: SimexServiceOuterClass.SubmitOrderResponse) = value.orderId
    }

    private class CancelOrderResponseObserver :
        BaseResponseObserver<SimexServiceOuterClass.CancelOrderResponse, CancelOrderResult>() {
        override fun map(value: SimexServiceOuterClass.CancelOrderResponse) =
            CancelOrderResult(value.succeed, value.errorMessage)
    }

    class CancelOrderResult(val succeed: Boolean, val errorMessage: String)

    private class GetBookResponseObserver : BaseResponseObserver<SimexServiceOuterClass.BookResponse, List<Order>>() {
        override fun map(value: SimexServiceOuterClass.BookResponse) =
            value.ordersList.stream().map { it.toOrder() }.toList()
    }

    private abstract class BaseResponseObserver<T : GeneratedMessageV3, R : Any> : StreamObserver<T> {
        protected val singleSubject: SingleSubject<R> = SingleSubject.create()
        val single: Single<R> get() = singleSubject

        override fun onNext(value: T) = singleSubject.onSuccess(map(value))
        protected abstract fun map(value: T): R
        override fun onError(t: Throwable) = singleSubject.onError(t)
        override fun onCompleted() { /* none */
        }
    }

    private class MarketDataObserver : StreamObserver<SimexServiceOuterClass.MarketEvent> {
        private val eventsPublisher = PublishSubject.create<MarketEvent>()
        val events: Observable<MarketEvent> get() = eventsPublisher

        override fun onNext(value: SimexServiceOuterClass.MarketEvent) = eventsPublisher.onNext(value.toMarketEvent())
        override fun onError(t: Throwable) = eventsPublisher.onError(t)
        override fun onCompleted() = eventsPublisher.onComplete()
    }

    fun submitOrder(order: Order): Single<Long> {
        val responseObserver = SubmitOrderResponseObserver()
        stub.submitOrder(order.toMessage(), responseObserver)
        return responseObserver.single
    }

    fun cancelOrder(order: Order): Single<CancelOrderResult> {
        val responseObserver = CancelOrderResponseObserver()
        stub.cancelOrder(
            SimexServiceOuterClass.CancelOrderRequest.newBuilder().setOrderId(order.id).build(),
            responseObserver
        )
        return responseObserver.single
    }

    fun requestMarketDataStream(tickers: List<String>): Observable<MarketEvent> {
        val marketDataObserver = MarketDataObserver()
        val outputStreamObserver = stub.getMarketDataStream(marketDataObserver)
        outputStreamObserver.onNext(SimexServiceOuterClass.MarketDataRequest.newBuilder().addAllTicker(tickers).build())

        return marketDataObserver.events.doOnDispose { outputStreamObserver.onCompleted() }
    }

    fun getBook(tickers: List<String>): Single<List<Order>> {
        val responseObserver = GetBookResponseObserver()
        stub.getBook(
            SimexServiceOuterClass.BookRequest.newBuilder().addAllTicker(tickers).build(),
            responseObserver
        )
        return responseObserver.single
    }
}