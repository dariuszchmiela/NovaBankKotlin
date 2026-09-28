package pl.dch.novabank

import com.jayway.jsonpath.JsonPath
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import pl.dch.novabank.entity.OutboxEvent
import pl.dch.novabank.entity.ProcessedTransfer
import pl.dch.novabank.repository.OutboxEventRepository
import pl.dch.novabank.repository.ProcessedTransferRepository
import pl.dch.novabank.service.OutboxPublisherScheduler
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

// REST -> outbox_events -> OutboxPublisherScheduler -> Kafka -> TransferConsumerService -> processed_transfers, nothing mocked
class TransferFlowEndToEndTest(
    @param:Autowired private val mockMvc: MockMvc,
    @param:Autowired private val outboxEventRepository: OutboxEventRepository,
    @param:Autowired private val outboxPublisherScheduler: OutboxPublisherScheduler,
    @param:Autowired private val processedTransferRepository: ProcessedTransferRepository
) : AbstractIntegrationTest() {

    private val sourceAccountId = "ACC-E2E-SRC-${UUID.randomUUID()}"
    private val targetAccountId = "ACC-E2E-TGT-${UUID.randomUUID()}"

    @Test
    fun `transfer request should flow from REST through outbox and Kafka to processed transfers`() {
        val responseBody = mockMvc.post("/api/transfers") {
            header("X-API-Version", "1")
            contentType = MediaType.APPLICATION_JSON
            content = """
                {
                  "sourceAccountId": "$sourceAccountId",
                  "targetAccountId": "$targetAccountId",
                  "amount": 150.00,
                  "currency": "$CURRENCY"
                }
            """.trimIndent()
        }.andExpect {
            status { isAccepted() }
            jsonPath("$.status") { value("ACCEPTED") }
        }.andReturn().response.contentAsString

        val transferId = JsonPath.read<String>(responseBody, "$.transferId")

        val pendingOutboxEvent = findOutboxEvent(transferId)
        assertEquals(sourceAccountId, pendingOutboxEvent.messageKey)
        assertTrue(pendingOutboxEvent.payload.contains(transferId))
        assertNull(pendingOutboxEvent.publishedAt)

        outboxPublisherScheduler.publishPendingEvents()

        val processedTransfer = awaitProcessedTransfer(UUID.fromString(transferId))
        assertEquals(sourceAccountId, processedTransfer.sourceAccountId)
        assertEquals(targetAccountId, processedTransfer.targetAccountId)
        assertEquals(AMOUNT, processedTransfer.amount)
        assertEquals(CURRENCY, processedTransfer.currency)
        assertNotNull(processedTransfer.processedAt)

        assertNotNull(outboxEventRepository.findById(pendingOutboxEvent.id).orElseThrow().publishedAt)
    }

    private fun findOutboxEvent(transferId: String): OutboxEvent =
        outboxEventRepository.findAll().single { it.payload.contains(transferId) }

    private fun awaitProcessedTransfer(transferId: UUID): ProcessedTransfer =
        await().atMost(AWAIT_TIMEOUT).until({ processedTransferRepository.findById(transferId).orElse(null) }, { it != null })

    companion object {
        private val AMOUNT = BigDecimal("150.00")
        private const val CURRENCY = "PLN"
        private val AWAIT_TIMEOUT = Duration.ofSeconds(30)
    }
}
