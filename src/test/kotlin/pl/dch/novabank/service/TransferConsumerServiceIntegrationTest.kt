package pl.dch.novabank.service

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import pl.dch.novabank.AbstractIntegrationTest
import pl.dch.novabank.config.KafkaTopicsProperties
import pl.dch.novabank.entity.ProcessedTransfer
import pl.dch.novabank.event.TransferRequestedEvent
import pl.dch.novabank.repository.ProcessedTransferRepository
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TransferConsumerServiceIntegrationTest(
    @param:Autowired private val kafkaTemplate: KafkaTemplate<Any, Any>,
    @param:Autowired private val kafkaTopicsProperties: KafkaTopicsProperties,
    @param:Autowired private val processedTransferRepository: ProcessedTransferRepository
) : AbstractIntegrationTest() {

    private val sourceAccountId = "ACC-CONSUMER-IT-${UUID.randomUUID()}"

    @BeforeEach
    fun cleanProcessedTransfers() {
        processedTransferRepository.deleteAllInBatch()
    }

    @Test
    fun `should persist transfer on first delivery`() {
        val transferId = UUID.randomUUID()

        publishEvent(buildEvent(transferId))

        val processedTransfer = awaitProcessedTransfer(transferId)
        assertEquals(transferId, processedTransfer.transferId)
        assertEquals(sourceAccountId, processedTransfer.sourceAccountId)
        assertEquals(TARGET_ACCOUNT_ID, processedTransfer.targetAccountId)
        assertEquals(AMOUNT, processedTransfer.amount)
        assertEquals(CURRENCY, processedTransfer.currency)
        assertNotNull(processedTransfer.processedAt)
    }

    @Test
    fun `should skip duplicate without overwriting it and keep processing subsequent messages`() {
        val duplicatedTransferId = UUID.randomUUID()
        val nextTransferId = UUID.randomUUID()

        publishEvent(buildEvent(duplicatedTransferId))
        val firstProcessedAt = awaitProcessedTransfer(duplicatedTransferId).processedAt

        // Same key -> same partition, so the duplicate is consumed before the next transfer
        publishEvent(buildEvent(duplicatedTransferId))
        publishEvent(buildEvent(nextTransferId))
        awaitProcessedTransfer(nextTransferId)

        assertEquals(firstProcessedAt, processedTransferRepository.findById(duplicatedTransferId).orElseThrow().processedAt)
        assertEquals(
            setOf(duplicatedTransferId, nextTransferId),
            processedTransferRepository.findAll().filter { it.sourceAccountId == sourceAccountId }.map { it.transferId }.toSet()
        )
    }

    private fun awaitProcessedTransfer(transferId: UUID): ProcessedTransfer =
        await().atMost(AWAIT_TIMEOUT).until({ processedTransferRepository.findById(transferId).orElse(null) }, { it != null })

    private fun publishEvent(event: TransferRequestedEvent) {
        kafkaTemplate.send(kafkaTopicsProperties.transferRequested, sourceAccountId, event).get()
    }

    private fun buildEvent(transferId: UUID) = TransferRequestedEvent(
        transferId = transferId.toString(),
        sourceAccountId = sourceAccountId,
        targetAccountId = TARGET_ACCOUNT_ID,
        amount = AMOUNT,
        currency = CURRENCY,
        requestedAt = Instant.now()
    )

    companion object {
        private const val TARGET_ACCOUNT_ID = "ACC-TARGET-CONSUMER-IT"
        private val AMOUNT = BigDecimal("500.00")
        private const val CURRENCY = "PLN"
        private val AWAIT_TIMEOUT = Duration.ofSeconds(30)
    }
}
