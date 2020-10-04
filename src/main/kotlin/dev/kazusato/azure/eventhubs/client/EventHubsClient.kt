package dev.kazusato.azure.eventhubs.client

import com.azure.core.amqp.AmqpTransportType
import com.azure.core.amqp.ProxyAuthenticationType
import com.azure.core.amqp.ProxyOptions
import com.azure.messaging.eventhubs.EventData
import com.azure.messaging.eventhubs.EventHubClientBuilder
import java.net.InetSocketAddress
import java.net.Proxy

fun main() {
    EventHubsClient().sendMsg()
}

class EventHubsClient {

    private val connStr = System.getenv("EVENTHUB_CONN_STR")

    private val proxyHost = System.getenv("HTTP_PROXY_HOST")

    private val proxyPort = System.getenv("HTTP_PROXY_PORT")?.toInt(10)

    fun sendMsg() {
        val builder = EventHubClientBuilder().connectionString(connStr, "testhub")
        configureProxy(builder)

        builder.buildProducerClient().use { producer ->
            val batch = producer.createBatch()
            (1..100).forEach { number ->
                batch.tryAdd(EventData("{\"text\": \"aaabbbccc${number}\"}"))
            }

            producer.send(batch)
        }
    }

    private fun configureProxy(builder: EventHubClientBuilder): EventHubClientBuilder {
        if (proxyHost != null && proxyPort != null) {
            // AMQP_WEB_SOCKETSの場合のみProxy指定可能
            builder.transportType(AmqpTransportType.AMQP_WEB_SOCKETS)
                .proxyOptions(
                    ProxyOptions(
                        ProxyAuthenticationType.NONE,
                        Proxy(Proxy.Type.HTTP, InetSocketAddress(proxyHost, proxyPort)),
                        null,
                        null
                    )
                )
        }

        return builder
    }

}