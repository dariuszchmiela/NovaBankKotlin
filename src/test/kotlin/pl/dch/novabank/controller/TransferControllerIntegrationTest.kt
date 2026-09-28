package pl.dch.novabank.controller

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import pl.dch.novabank.AbstractIntegrationTest
import pl.dch.novabank.repository.OutboxEventRepository
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@AutoConfigureMockMvc
class TransferControllerIntegrationTest(
    @param:Autowired private val mockMvc: MockMvc,
    @param:Autowired private val outboxEventRepository: OutboxEventRepository
) : AbstractIntegrationTest() {

    @Test
    fun `should return 202 with transferId and save event to outbox when request is valid`() {
        val body = transferRequestJson(sourceAccountId = SOURCE_ACCOUNT_ID)

        mockMvc.post("/api/transfers") {
            header(API_VERSION_HEADER, "1")
            contentType = MediaType.APPLICATION_JSON
            content = body
        }.andExpect {
            status { isAccepted() }
            jsonPath("$.transferId") { exists() }
            jsonPath("$.status") { value("ACCEPTED") }
        }

        val savedEvents = outboxEventRepository.findAll().filter { it.messageKey == SOURCE_ACCOUNT_ID }
        assertEquals(1, savedEvents.size)
        assertEquals("bank.transfers.requested", savedEvents.single().topic)
        assertTrue(savedEvents.single().payload.contains(TARGET_ACCOUNT_ID))
        assertNull(savedEvents.single().publishedAt)
    }

    @Test
    fun `should return 400 with ProblemDetail when sourceAccountId is blank`() {
        val body = transferRequestJson(sourceAccountId = "")

        mockMvc.post("/api/transfers") {
            header(API_VERSION_HEADER, "1")
            contentType = MediaType.APPLICATION_JSON
            content = body
        }.andExpect {
            status { isBadRequest() }
            jsonPath("$.title") { value("Validation failed") }
            jsonPath("$.detail") { value("One or more fields are invalid") }
            jsonPath("$.errors.sourceAccountId") { exists() }
        }
    }

    private fun transferRequestJson(sourceAccountId: String) =
        """
        {
          "sourceAccountId": "$sourceAccountId",
          "targetAccountId": "$TARGET_ACCOUNT_ID",
          "amount": 75.00,
          "currency": "PLN"
        }
        """.trimIndent()

    companion object {
        private const val SOURCE_ACCOUNT_ID = "ACC-SOURCE-HTTP"
        private const val TARGET_ACCOUNT_ID = "ACC-TARGET-HTTP"
        private const val API_VERSION_HEADER = "X-API-Version"
    }
}
