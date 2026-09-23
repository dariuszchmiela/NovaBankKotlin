package pl.dch.novabank.service

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import pl.dch.novabank.config.KafkaTopicsProperties
import pl.dch.novabank.dto.TransferRequest
import pl.dch.novabank.event.TransferRequestedEvent
import pl.dch.novabank.repository.OutboxEventRepository
import tools.jackson.databind.ObjectMapper
import java.time.Instant
import java.util.UUID

@Service
class TransferProducerService(
    private val outboxEventRepository: OutboxEventRepository,
    private val kafkaTopicsProperties: KafkaTopicsProperties,
    @Qualifier("outboxObjectMapper")
    private val outboxObjectMapper: ObjectMapper
){
    private fun buildEvent(request: TransferRequest): TransferRequestedEvent =
        TransferRequestedEvent(
            transferId = UUID.randomUUID().toString(),
            sourceAccountId = request.sourceAccountId,
            targetAccountId = request.targetAccountId,
            amount = request.amount,
            currency = request.currency,
            requestedAt = Instant.now()
        )
}