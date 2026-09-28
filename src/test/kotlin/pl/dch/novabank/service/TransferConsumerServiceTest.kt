package pl.dch.novabank.service

import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.support.Acknowledgment
import pl.dch.novabank.entity.ProcessedTransfer
import pl.dch.novabank.event.TransferRequestedEvent
import pl.dch.novabank.repository.ProcessedTransferRepository
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class TransferConsumerServiceTest {

    private val processedTransferRepository = mock(ProcessedTransferRepository::class.java)
    private val acknowledgment = mock(Acknowledgment::class.java)
    private val transferConsumerService = TransferConsumerService(processedTransferRepository)

    @Test
    fun `should save processed transfer and acknowledge`() {
        val transferId = UUID.randomUUID()

        transferConsumerService.consumeTransferRequested(event(transferId.toString()), acknowledgment)

        val captor = ArgumentCaptor.forClass(ProcessedTransfer::class.java)
        verify(processedTransferRepository).save(captor.capture())
        with(captor.value) {
            assertEquals(transferId, this.transferId)
            assertEquals(SOURCE_ACCOUNT_ID, sourceAccountId)
            assertEquals(TARGET_ACCOUNT_ID, targetAccountId)
            assertEquals(AMOUNT, amount)
            assertEquals(CURRENCY, currency)
        }
        verify(acknowledgment).acknowledge()
    }

    @Test
    fun `should treat duplicate as success and acknowledge`() {
        val transferId = UUID.randomUUID()
        `when`(processedTransferRepository.save(any(ProcessedTransfer::class.java)))
            .thenThrow(DataIntegrityViolationException("duplicate key"))
        `when`(processedTransferRepository.existsById(transferId)).thenReturn(true)

        transferConsumerService.consumeTransferRequested(event(transferId.toString()), acknowledgment)

        verify(acknowledgment).acknowledge()
    }

    @Test
    fun `should rethrow and not acknowledge when integrity violation is not a duplicate`() {
        val transferId = UUID.randomUUID()
        `when`(processedTransferRepository.save(any(ProcessedTransfer::class.java)))
            .thenThrow(DataIntegrityViolationException("not null violation"))
        `when`(processedTransferRepository.existsById(transferId)).thenReturn(false)

        assertFailsWith<DataIntegrityViolationException> {
            transferConsumerService.consumeTransferRequested(event(transferId.toString()), acknowledgment)
        }

        verify(acknowledgment, never()).acknowledge()
    }

    @Test
    fun `should fail without saving or acknowledging when transferId is not a UUID`() {
        assertFailsWith<IllegalArgumentException> {
            transferConsumerService.consumeTransferRequested(event("not-a-valid-uuid"), acknowledgment)
        }

        verify(processedTransferRepository, never()).save(any(ProcessedTransfer::class.java))
        verify(acknowledgment, never()).acknowledge()
    }

    private fun event(transferId: String) = TransferRequestedEvent(
        transferId = transferId,
        sourceAccountId = SOURCE_ACCOUNT_ID,
        targetAccountId = TARGET_ACCOUNT_ID,
        amount = AMOUNT,
        currency = CURRENCY,
        requestedAt = Instant.now()
    )

    companion object {
        private const val SOURCE_ACCOUNT_ID = "ACC-SOURCE-1"
        private const val TARGET_ACCOUNT_ID = "ACC-TARGET-1"
        private val AMOUNT = BigDecimal("500.00")
        private const val CURRENCY = "PLN"
    }
}
