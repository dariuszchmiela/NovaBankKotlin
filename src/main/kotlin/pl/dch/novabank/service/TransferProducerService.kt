package pl.dch.novabank.service

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import pl.dch.novabank.config.KafkaTopicsProperties
import pl.dch.novabank.dto.TransferRequest
import pl.dch.novabank.entity.OutboxEvent
import pl.dch.novabank.event.TransferRequestedEvent
import pl.dch.novabank.repository.OutboxEventRepository
import tools.jackson.core.JacksonException
import tools.jackson.databind.ObjectMapper
import java.time.Instant
import java.util.UUID

@Service
class TransferProducerService(
    private val outboxEventRepository: OutboxEventRepository,
    private val kafkaTopicsProperties: KafkaTopicsProperties,
    @Qualifier("outboxObjectMapper")
    private val outboxObjectMapper: ObjectMapper
) {

    @Transactional
    fun publishTransferRequested(request: TransferRequest): String {
        val event = buildEvent(request)

        val outboxEvent = OutboxEvent(
            id = UUID.randomUUID(),
            topic = kafkaTopicsProperties.transferRequested,
            messageKey = event.sourceAccountId,
            payload = serialize(event),
            createdAt = Instant.now()
        )

        outboxEventRepository.save(outboxEvent)

        log.info("Saved TransferRequestedEvent transferId={} to outbox", event.transferId)

        return event.transferId
    }

    private fun buildEvent(request: TransferRequest): TransferRequestedEvent =
        TransferRequestedEvent(
            transferId = UUID.randomUUID().toString(),
            sourceAccountId = request.sourceAccountId,
            targetAccountId = request.targetAccountId,
            amount = request.amount,
            currency = request.currency,
            requestedAt = Instant.now()
        )

    private fun serialize(event: TransferRequestedEvent): String =
        try {
            outboxObjectMapper.writeValueAsString(event)
        } catch (exception: JacksonException) {
            throw IllegalStateException("Failed to serialize TransferRequestedEvent transferId=${event.transferId}", exception)
        }

    companion object {
        private val log = LoggerFactory.getLogger(TransferProducerService::class.java)
    }
}
