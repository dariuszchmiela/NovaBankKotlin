package pl.dch.novabank.service

import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service
import pl.dch.novabank.entity.ProcessedTransfer
import pl.dch.novabank.event.TransferRequestedEvent
import pl.dch.novabank.repository.ProcessedTransferRepository
import java.time.Instant
import java.util.UUID

@Service
class TransferConsumerService(
    private val processedTransferRepository: ProcessedTransferRepository
) {

    @KafkaListener(topics = ["\${novabank.kafka.topics.transfer-requested}"])
    fun consumeTransferRequested(event: TransferRequestedEvent, acknowledgment: Acknowledgment) {
        processTransfer(event)
        acknowledgment.acknowledge()
    }

    private fun processTransfer(event: TransferRequestedEvent) {
        val transferId = UUID.fromString(event.transferId)

        try {
            processedTransferRepository.save(toEntity(event, transferId))
            log.info("Processed transferId={} sourceAccountId={}", event.transferId, event.sourceAccountId)
        } catch (exception: DataIntegrityViolationException) {
            if (!processedTransferRepository.existsById(transferId)) {
                throw exception
            }
            log.info("Skipping duplicate transferId={} - already processed", event.transferId)
        }
    }

    private fun toEntity(event: TransferRequestedEvent, transferId: UUID) =
        ProcessedTransfer(
            transferId = transferId,
            sourceAccountId = event.sourceAccountId,
            targetAccountId = event.targetAccountId,
            amount = event.amount,
            currency = event.currency,
            processedAt = Instant.now()
        )

    companion object {
        private val log = LoggerFactory.getLogger(TransferConsumerService::class.java)
    }
}
