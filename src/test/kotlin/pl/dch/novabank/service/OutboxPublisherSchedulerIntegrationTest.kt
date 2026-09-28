package pl.dch.novabank.service

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import pl.dch.novabank.AbstractIntegrationTest
import pl.dch.novabank.config.KafkaTopicsProperties
import pl.dch.novabank.entity.OutboxEvent
import pl.dch.novabank.repository.OutboxEventRepository
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class OutboxPublisherSchedulerIntegrationTest(
    @param:Autowired private val outboxPublisherScheduler: OutboxPublisherScheduler,
    @param:Autowired private val outboxEventRepository: OutboxEventRepository,
    @param:Autowired private val kafkaTopicsProperties: KafkaTopicsProperties
) : AbstractIntegrationTest() {

    private val testConsumer = KafkaConsumer<String, String>(
        mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "outbox-publisher-it-${UUID.randomUUID()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )
    ).apply { subscribe(listOf(kafkaTopicsProperties.transferRequested)) }

    @AfterEach
    fun tearDownConsumer() {
        testConsumer.close()
    }

    @Test
    fun `should publish pending outbox event to Kafka once and mark it as published`() {
        val messageKey = "ACC-OUTBOX-IT-${UUID.randomUUID()}"
        val payload = """{"transferId":"${UUID.randomUUID()}","sourceAccountId":"$messageKey","amount":42.00}"""
        val outboxEvent = outboxEventRepository.save(
            OutboxEvent(
                id = UUID.randomUUID(),
                topic = kafkaTopicsProperties.transferRequested,
                messageKey = messageKey,
                payload = payload,
                createdAt = Instant.now()
            )
        )

        outboxPublisherScheduler.publishPendingEvents()

        val received = pollRecordsWithKey(messageKey, POLL_TIMEOUT) { it.isNotEmpty() }
        assertEquals(1, received.size)
        assertEquals(messageKey, received.single().key())
        assertEquals(payload, received.single().value())

        val publishedAt = outboxEventRepository.findById(outboxEvent.id).orElseThrow().publishedAt
        assertNotNull(publishedAt)

        outboxPublisherScheduler.publishPendingEvents()

        val republished = pollRecordsWithKey(messageKey, NO_REPUBLISH_WINDOW) { false }
        assertTrue(republished.isEmpty(), "Already published event must not be sent again")
        assertEquals(publishedAt, outboxEventRepository.findById(outboxEvent.id).orElseThrow().publishedAt)
    }

    private fun pollRecordsWithKey(
        key: String,
        timeout: Duration,
        until: (List<ConsumerRecord<String, String>>) -> Boolean
    ): List<ConsumerRecord<String, String>> {
        val deadline = Instant.now().plus(timeout)
        val received = mutableListOf<ConsumerRecord<String, String>>()

        while (Instant.now().isBefore(deadline) && !until(received)) {
            testConsumer.poll(Duration.ofMillis(500))
                .records(kafkaTopicsProperties.transferRequested)
                .filterTo(received) { it.key() == key }
        }

        return received
    }

    companion object {
        private val POLL_TIMEOUT = Duration.ofSeconds(15)
        private val NO_REPUBLISH_WINDOW = Duration.ofSeconds(3)
    }
}
