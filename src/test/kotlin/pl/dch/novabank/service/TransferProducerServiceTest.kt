package pl.dch.novabank.service

import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import pl.dch.novabank.config.KafkaTopicsProperties
import pl.dch.novabank.config.OutboxObjectMapperConfig
import pl.dch.novabank.dto.TransferRequest
import pl.dch.novabank.entity.OutboxEvent
import pl.dch.novabank.repository.OutboxEventRepository
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TransferProducerServiceTest {

    private val outboxEventRepository = mock(OutboxEventRepository::class.java)

    private val transferProducerService = TransferProducerService(
        outboxEventRepository = outboxEventRepository,
        kafkaTopicsProperties = KafkaTopicsProperties(TRANSFER_REQUESTED_TOPIC, TRANSFER_REQUESTED_DLT_TOPIC),
        outboxObjectMapper = OutboxObjectMapperConfig().outboxObjectMapper()
    )

    @Test
    fun `publishTransferRequested should save event to outbox`() {
        val request = TransferRequest(SOURCE_ACCOUNT_ID, TARGET_ACCOUNT_ID, AMOUNT, CURRENCY)

        val transferId = transferProducerService.publishTransferRequested(request)

        val outboxEventCaptor = ArgumentCaptor.forClass(OutboxEvent::class.java)
        verify(outboxEventRepository).save(outboxEventCaptor.capture())

        val savedOutboxEvent = outboxEventCaptor.value
        assertEquals(TRANSFER_REQUESTED_TOPIC, savedOutboxEvent.topic)
        assertEquals(SOURCE_ACCOUNT_ID, savedOutboxEvent.messageKey)
        assertTrue(savedOutboxEvent.payload.contains(transferId))
        assertTrue(savedOutboxEvent.payload.contains(SOURCE_ACCOUNT_ID))
        assertTrue(savedOutboxEvent.payload.contains(TARGET_ACCOUNT_ID))
        assertTrue(savedOutboxEvent.payload.contains(CURRENCY))
        assertNull(savedOutboxEvent.publishedAt)
    }

    companion object {
        private const val SOURCE_ACCOUNT_ID = "ACC-SOURCE-1"
        private const val TARGET_ACCOUNT_ID = "ACC-TARGET-1"
        private val AMOUNT = BigDecimal("150.00")
        private const val CURRENCY = "PLN"
        private const val TRANSFER_REQUESTED_TOPIC = "bank.transfers.requested"
        private const val TRANSFER_REQUESTED_DLT_TOPIC = "bank.transfers.requested.dlt"
    }
}
