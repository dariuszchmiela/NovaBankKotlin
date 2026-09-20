package pl.dch.novabank.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "outbox_events")
class OutboxEvent(
    @Id
    @Column(name = "id")
    val id: UUID,

    @Column(name = "topic", nullable = false)
    val topic: String,

    @Column(name = "message_key", nullable = false)
    val messageKey: String,

    @Column(name = "payload", nullable = false)
    val payload: String,

    @Column(name = "created_at", nullable = false)
    val createdAt: Instant,

    @Column(name = "published_at")
    var publishedAt: Instant? = null
) {
    fun markPublished(publishedAt: Instant) {
        this.publishedAt = publishedAt
    }
}