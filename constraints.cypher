// Generated Cypher constraints for KnownetApplicationProfile

// Unique constraint for class: Contract
CREATE CONSTRAINT Contract_id_unique IF NOT EXISTS
FOR (n:`Contract`)
REQUIRE n.`id` IS UNIQUE;

// Unique constraint for class: Tender
CREATE CONSTRAINT Tender_id_unique IF NOT EXISTS
FOR (n:`Tender`)
REQUIRE n.`id` IS UNIQUE;

// Unique constraint for class: Location
CREATE CONSTRAINT Location_id_unique IF NOT EXISTS
FOR (n:`Location`)
REQUIRE n.`id` IS UNIQUE;

// Unique constraint for class: CPV
CREATE CONSTRAINT CPV_id_unique IF NOT EXISTS
FOR (n:`CPV`)
REQUIRE n.`id` IS UNIQUE;

// Unique constraint for class: Document
CREATE CONSTRAINT Document_id_unique IF NOT EXISTS
FOR (n:`Document`)
REQUIRE n.`id` IS UNIQUE;

// Unique constraint for class: Entity
CREATE CONSTRAINT Entity_id_unique IF NOT EXISTS
FOR (n:`Entity`)
REQUIRE n.`id` IS UNIQUE;

// Unique constraint for class: Person
CREATE CONSTRAINT Person_id_unique IF NOT EXISTS
FOR (n:`Person`)
REQUIRE n.`id` IS UNIQUE;

