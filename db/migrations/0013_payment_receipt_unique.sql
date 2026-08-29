-- Pay-once-board-many guard: one switch settlement receipt may back exactly
-- one booking. The partial unique index covers every paid booking while
-- leaving unpaid bookings (NULL receipt) unconstrained. The store maps a
-- violation of this index to a 409 idempotency/reuse conflict.
CREATE UNIQUE INDEX truck_bookings_payment_receipt_ref_uniq
    ON truck_bookings (payment_receipt_ref)
    WHERE payment_receipt_ref IS NOT NULL;
