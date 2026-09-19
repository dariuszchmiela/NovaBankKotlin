package pl.dch.novabank.dto

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import java.math.BigDecimal


data class TransferRequest(
    @field:NotBlank
    val sourceAccountId: String,
    @field:NotBlank
    val targetAccountId: String,
    @field:NotNull
    val amount: BigDecimal,
    @field:NotBlank
    val currency: String
)