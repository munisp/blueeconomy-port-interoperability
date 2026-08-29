-- Refund rail terminal state: a PAID (or VALIDATION_PENDING) booking that is
-- cancelled or expires is refunded through the compensating TigerBeetle
-- transfer and lands in REFUNDED — money is never stranded in a terminal
-- state that forgot the trucker paid.
ALTER TABLE truck_bookings DROP CONSTRAINT truck_bookings_status_check;
ALTER TABLE truck_bookings
    ADD CONSTRAINT truck_bookings_status_check
    CHECK (status IN (
        'DRAFTED', 'PENDING_SYNC', 'SLOT_RESERVED', 'PAID', 'VALIDATION_PENDING',
        'GATE_APPROVED', 'COMPLETED', 'CANCELLED', 'EXPIRED', 'REJECTED',
        'RECONCILIATION_REQUIRED', 'REFUNDED'
    ));
