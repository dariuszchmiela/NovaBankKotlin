package pl.dch.novabank.controller

import jakarta.validation.Valid
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import pl.dch.novabank.dto.TransferRequest
import pl.dch.novabank.service.TransferProducerService

@RestController
@RequestMapping("/api/transfers")
class TransferController(
    private val transferProducerService: TransferProducerService
) {

    @PostMapping(version = "1")
    fun requestTransfer(@Valid @RequestBody request: TransferRequest): ResponseEntity<Map<String, String>> {
        log.info("Received transfer request from account {}", request.sourceAccountId)
        val transferId = transferProducerService.publishTransferRequested(request)
        return ResponseEntity.status(HttpStatus.ACCEPTED)
            .body(mapOf("transferId" to transferId, "status" to "ACCEPTED"))
    }

    companion object {
        private val log = LoggerFactory.getLogger(TransferController::class.java)
    }
}
