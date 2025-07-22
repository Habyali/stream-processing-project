
CREATE TABLE content (
    id              UUID PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT        NOT NULL,
    content_type    TEXT        NOT NULL,  
    length_seconds  INTEGER,
    publish_ts      TIMESTAMPTZ NOT NULL
);


CREATE TABLE engagement_events (
    id           BIGSERIAL PRIMARY KEY,
    content_id   UUID REFERENCES content(id),
    user_id      UUID,
    event_type   TEXT NOT NULL,  
    event_ts     TIMESTAMPTZ NOT NULL,
    duration_ms  INTEGER,
    device       TEXT,
    raw_payload  JSONB
);


CREATE INDEX idx_content_type ON content(content_type);
CREATE INDEX idx_content_publish_ts ON content(publish_ts);

CREATE INDEX idx_engagement_content_id ON engagement_events(content_id);
CREATE INDEX idx_engagement_event_type ON engagement_events(event_type);
CREATE INDEX idx_engagement_event_ts ON engagement_events(event_ts);
CREATE INDEX idx_engagement_user_id ON engagement_events(user_id);
CREATE INDEX idx_engagement_device ON engagement_events(device);


CREATE INDEX idx_engagement_content_event ON engagement_events(content_id, event_type);
CREATE INDEX idx_engagement_type_time ON engagement_events(event_type, event_ts);