-- Booking ownership for access control: the creating subject (from verified
-- token claims) is recorded at insert so reads can be scoped to the owner.
-- Nullable: rows created before this migration have no recorded owner and are
-- readable by officer roles only.
ALTER TABLE truck_bookings ADD COLUMN created_by TEXT;
