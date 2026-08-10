package com.mohammadanas.saga.inventory.domain;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface InventoryRepository extends JpaRepository<Inventory, String> {

    /**
     * Loads a stock row under a write lock.
     *
     * <p>Messages are partitioned by saga, so two sagas competing for the same item are
     * handled concurrently by different consumer threads. Without this lock both can read
     * the same {@code availableQuantity}, both pass the check, and the item is oversold.
     * A pessimistic lock serialises per item deterministically, whereas an optimistic
     * version would surface as a retry the consumer is not yet configured to handle.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select i from Inventory i where i.itemId = :itemId")
    Optional<Inventory> findByItemIdForUpdate(@Param("itemId") String itemId);
}
