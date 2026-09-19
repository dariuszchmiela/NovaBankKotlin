package pl.dch.novabank.dto

import java.math.BigDecimal


data class TransferRequest(
    val sourceAccountId: String,
    val targetAccountId: String,
    val amount: BigDecimal,
    val currency: String
)