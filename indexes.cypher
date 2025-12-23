// Unique constraint for class: TenurePeriod
CREATE INDEX TenurePeriod_startDate IF NOT EXISTS
FOR (n:`TenurePeriod`)
ON n.`startDate`;

CREATE INDEX TenurePeriod_endDate IF NOT EXISTS
FOR (n:`TenurePeriod`)
ON n.`endDate`;