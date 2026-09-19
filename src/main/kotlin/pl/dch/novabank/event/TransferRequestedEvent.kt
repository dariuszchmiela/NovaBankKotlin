package pl.dch.novabank.event

import java.math.BigDecimal
import java.time.Instant

data class TransferRequestedEvent(
    val transferId: String,
    val sourceAccountId: String,
    val targetAccountId: String,
    val amount: BigDecimal,
    val currency: String,
    val requestedAt: Instant
)