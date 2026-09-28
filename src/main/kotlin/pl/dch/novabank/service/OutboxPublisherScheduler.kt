package pl.dch.novabank.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import pl.dch.novabank.config.OutboxProperties
import pl.dch.novabank.entity.OutboxEvent
import pl.dch.novabank.repository.OutboxEventRepository
import java.time.Instant
import java.util.concurrent.TimeUnit

@Component
class OutboxPublisherScheduler(
    private val outboxEventRepository: OutboxEventRepository,
    @Qualifier("outboxKafkaTemplate")
    private val outboxKafkaTemplate: KafkaTemplate<String, String>,
    private val outboxProperties: OutboxProperties
) {

    @Scheduled(
        fixedDelayString = "\${novabank.outbox.poll-interval-ms}",
        initialDelayString = "\${novabank.outbox.poll-interval-ms}"
    )
    @Transactional
    fun publishPendingEvents() {
        outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc()
            .forEach(::publishEvent)
    }

    private fun publishEvent(outboxEvent: OutboxEvent) {
        try {
            outboxKafkaTemplate.send(outboxEvent.topic, outboxEvent.messageKey, outboxEvent.payload)
                .get(outboxProperties.publishTimeoutSeconds, TimeUnit.SECONDS)

            outboxEvent.markPublished(Instant.now())
            outboxEventRepository.save(outboxEvent)

            log.info("Published outbox event id={} topic={}", outboxEvent.id, outboxEvent.topic)
        } catch (exception: Exception) {
            log.warn("Failed to publish outbox event id={}, will retry on next poll", outboxEvent.id, exception)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(OutboxPublisherScheduler::class.java)
    }
}
