package pl.dch.novabank.repository

import org.springframework.data.jpa.repository.JpaRepository
import pl.dch.novabank.entity.OutboxEvent
import java.util.UUID

interface OutboxEventRepository : JpaRepository<OutboxEvent, UUID> {

    fun findByPublishedAtIsNullOrderByCreatedAtAsc(): List<OutboxEvent>
}
