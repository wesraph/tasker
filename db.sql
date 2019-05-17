DROP TABLE IF EXISTS "tasks" ;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TYPE IF EXISTS task_status;
CREATE TYPE task_status AS ENUM ('todo', 'error', 'done');

CREATE TABLE "tasks" (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at timestamp DEFAULT NOW() NOT NULL,
    todo_date timestamp DEFAULT NOW() NOT NULL,
    name VARCHAR(255) NOT NULL,
    actual_step VARCHAR(255) NOT NULL,
    status task_status DEFAULT 'todo' NOT NULL,
    retry int DEFAULT 0 NOT NULL,
    buffer JSON,
    args JSON
);
