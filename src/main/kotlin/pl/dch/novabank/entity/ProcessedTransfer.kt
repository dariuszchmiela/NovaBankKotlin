package pl.dch.novabank.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.domain.Persistable
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "processed_transfers")
class ProcessedTransfer(
    @Id
    @Column(name = "transfer_id")
    val transferId: UUID,

    @Column(name = "source_account_id", nullable = false)
    val sourceAccountId: String,

    @Column(name = "target_account_id", nullable = false)
    val targetAccountId: String,

    @Column(name = "amount", nullable = false)
    val amount: BigDecimal,

    @Column(name = "currency", nullable = false)
    val currency: String,

    @Column(name = "processed_at", nullable = false)
    val processedAt: Instant
) : Persistable<UUID> {

    override fun getId(): UUID = transferId

    // Always INSERT (persist) so a redelivered transferId hits the primary key instead of being merged over the existing row
    override fun isNew(): Boolean = true
}
