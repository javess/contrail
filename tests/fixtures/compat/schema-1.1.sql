PRAGMA application_id = 1129599564;
PRAGMA journal_mode = DELETE;
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE manifest (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT INTO manifest VALUES('schema_version','1.1');
INSERT INTO manifest VALUES('producer_version','0.1.0');
CREATE TABLE executions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    started_at_ns INTEGER NOT NULL,
    finished_at_ns INTEGER,
    command_json TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    exit_code INTEGER,
    revision TEXT,
    metadata_json TEXT NOT NULL
);
INSERT INTO executions VALUES('fixture-run','compatibility-fixture',100,200,'["python","fixture.py"]','/compat',0,'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','{"fixture":"schema-1.1"}');
CREATE TABLE entities (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    parent_entity_id TEXT REFERENCES entities(id),
    attributes_json TEXT NOT NULL
);
INSERT INTO entities VALUES('fixture-process','process','python',NULL,'{"fixture":true}');
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    entity_id TEXT REFERENCES entities(id),
    started_at_ns INTEGER,
    finished_at_ns INTEGER,
    clock_domain TEXT,
    uncertainty_ns INTEGER,
    sequence INTEGER,
    attributes_json TEXT NOT NULL,
    CHECK (finished_at_ns IS NULL OR started_at_ns IS NULL OR finished_at_ns >= started_at_ns),
    CHECK (uncertainty_ns IS NULL OR uncertainty_ns >= 0)
);
INSERT INTO events VALUES('fixture-event','operation','fixture.work','fixture-process',120,180,'fixture.clock',0,1,'{"fixture":true}');
CREATE TABLE causal_edges (
    source_event_id TEXT NOT NULL REFERENCES events(id),
    target_event_id TEXT NOT NULL REFERENCES events(id),
    kind TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    attributes_json TEXT NOT NULL,
    PRIMARY KEY (source_event_id, target_event_id, kind),
    CHECK (confidence >= 0.0 AND confidence <= 1.0)
);
CREATE TABLE measurements (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    timestamp_ns INTEGER,
    entity_id TEXT REFERENCES entities(id),
    attributes_json TEXT NOT NULL
);
INSERT INTO measurements VALUES(1,'fixture.value',1.5,'1',150,'fixture-process','{"fixture":true}');
CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    content BLOB NOT NULL,
    attributes_json TEXT NOT NULL
);
INSERT INTO attachments VALUES('fixture-attachment','raw','fixture.txt','text/plain',X'666978747572652065766964656e6365','{"fixture":true}');
CREATE INDEX events_time_idx ON events(started_at_ns, finished_at_ns);
CREATE INDEX events_entity_idx ON events(entity_id);
CREATE INDEX events_semantic_idx ON events(kind, name);
CREATE INDEX edges_target_idx ON causal_edges(target_event_id);
CREATE INDEX measurements_name_time_idx ON measurements(name, timestamp_ns);
CREATE INDEX measurements_entity_idx ON measurements(entity_id);
CREATE INDEX attachments_kind_name_idx ON attachments(kind, name);
COMMIT;
