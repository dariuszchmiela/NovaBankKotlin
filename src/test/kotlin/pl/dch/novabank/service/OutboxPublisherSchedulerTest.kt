package pl.dch.novabank.service

import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.transaction.annotation.Transactional
import pl.dch.novabank.config.OutboxProperties
import pl.dch.novabank.entity.OutboxEvent
import pl.dch.novabank.repository.OutboxEventRepository
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class OutboxPublisherSchedulerTest {

    private val outboxEventRepository = mock(OutboxEventRepository::class.java)

    @Suppress("UNCHECKED_CAST")
    private val outboxKafkaTemplate = mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>

    private val outboxPublisherScheduler = OutboxPublisherScheduler(
        outboxEventRepository = outboxEventRepository,
        outboxKafkaTemplate = outboxKafkaTemplate,
        outboxProperties = OutboxProperties(pollIntervalMs = 1000, publishTimeoutSeconds = 1)
    )

    @Test
    fun `should mark event as published and save it when Kafka send succeeds`() {
        val outboxEvent = pendingOutboxEvent()
        `when`(outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(listOf(outboxEvent))
        `when`(outboxKafkaTemplate.send(TOPIC, MESSAGE_KEY, PAYLOAD))
            .thenReturn(CompletableFuture.completedFuture(null))

        outboxPublisherScheduler.publishPendingEvents()

        verify(outboxKafkaTemplate).send(TOPIC, MESSAGE_KEY, PAYLOAD)
        assertNotNull(outboxEvent.publishedAt)
        verify(outboxEventRepository).save(outboxEvent)
    }

    @Test
    fun `should leave event unpublished and not throw when Kafka send fails`() {
        val outboxEvent = pendingOutboxEvent()
        `when`(outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(listOf(outboxEvent))
        `when`(outboxKafkaTemplate.send(TOPIC, MESSAGE_KEY, PAYLOAD))
            .thenReturn(CompletableFuture.failedFuture(IllegalStateException("Kafka unavailable")))

        outboxPublisherScheduler.publishPendingEvents()

        assertNull(outboxEvent.publishedAt)
        verify(outboxEventRepository, never()).save(outboxEvent)
    }

    @Test
    fun `should leave event unpublished when Kafka send times out`() {
        val outboxEvent = pendingOutboxEvent()
        `when`(outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc()).thenReturn(listOf(outboxEvent))
        `when`(outboxKafkaTemplate.send(TOPIC, MESSAGE_KEY, PAYLOAD))
            .thenReturn(CompletableFuture<SendResult<String, String>>())

        outboxPublisherScheduler.publishPendingEvents()

        assertNull(outboxEvent.publishedAt)
        verify(outboxEventRepository, never()).save(outboxEvent)
    }

    @Test
    fun `should continue publishing remaining events after one fails`() {
        val failingEvent = pendingOutboxEvent(messageKey = FAILING_MESSAGE_KEY)
        val succeedingEvent = pendingOutboxEvent()
        `when`(outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc())
            .thenReturn(listOf(failingEvent, succeedingEvent))
        `when`(outboxKafkaTemplate.send(TOPIC, FAILING_MESSAGE_KEY, PAYLOAD))
            .thenReturn(CompletableFuture.failedFuture(IllegalStateException("Kafka unavailable")))
        `when`(outboxKafkaTemplate.send(TOPIC, MESSAGE_KEY, PAYLOAD))
            .thenReturn(CompletableFuture.completedFuture(null))

        outboxPublisherScheduler.publishPendingEvents()

        assertNull(failingEvent.publishedAt)
        assertNotNull(succeedingEvent.publishedAt)
        verify(outboxEventRepository, never()).save(failingEvent)
        verify(outboxEventRepository).save(succeedingEvent)
    }

    @Test
    fun `publishPendingEvents should be transactional and scheduled with configured fixed delay`() {
        val method = OutboxPublisherScheduler::class.java.getMethod("publishPendingEvents")

        val scheduled = assertNotNull(method.getAnnotation(Scheduled::class.java))
        assertEquals("\${novabank.outbox.poll-interval-ms}", scheduled.fixedDelayString)
        assertEquals("\${novabank.outbox.poll-interval-ms}", scheduled.initialDelayString)
        assertNotNull(method.getAnnotation(Transactional::class.java))
    }

    private fun pendingOutboxEvent(messageKey: String = MESSAGE_KEY) = OutboxEvent(
        id = UUID.randomUUID(),
        topic = TOPIC,
        messageKey = messageKey,
        payload = PAYLOAD,
        createdAt = Instant.now()
    )

    companion object {
        private const val TOPIC = "bank.transfers.requested"
        private const val MESSAGE_KEY = "ACC-SOURCE-1"
        private const val FAILING_MESSAGE_KEY = "ACC-FAILING"
        private const val PAYLOAD = """{"transferId":"t-1","sourceAccountId":"ACC-SOURCE-1"}"""
    }
}
