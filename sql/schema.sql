-- Makes schema changes: creates the tables, views and procedures for the online migration rebuild.
-- migration/db.py swaps the database name below when it builds a database with another name (the tests use one).
USE [RebuildOnlineMigrations];
GO

-- Lets the consistency checks read one point in time while requests keep writing (see controller.py).
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
GO

-- The old store: each customer's subscriptions live inside the customer row as a JSON array,
-- like the subscriptions array on Stripe's Customer object.
CREATE TABLE dbo.Customers (
    CustomerId        INT IDENTITY(1, 1) NOT NULL CONSTRAINT PK_Customers PRIMARY KEY,
    Email             NVARCHAR(320)      NOT NULL,
    SubscriptionsJson NVARCHAR(MAX)      NULL,
    MigratedAt        DATETIME2(3)       NULL
);
GO

-- The new store: one row per subscription.
CREATE TABLE dbo.Subscriptions (
    SubscriptionId UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_Subscriptions PRIMARY KEY NONCLUSTERED,
    CustomerId     INT              NOT NULL CONSTRAINT FK_Subscriptions_Customers REFERENCES dbo.Customers (CustomerId),
    PlanCode       VARCHAR(20)      NOT NULL,
    Status         VARCHAR(20)      NOT NULL,
    CreatedAt      DATETIME2(3)     NOT NULL
);
CREATE CLUSTERED INDEX CX_Subscriptions_CustomerId ON dbo.Subscriptions (CustomerId);
GO

-- One row that says where the migration is. Every request reads it with a shared lock held to the
-- end of its transaction, so changing the phase waits for requests already in flight.
CREATE TABLE dbo.MigrationState (
    StateId          TINYINT NOT NULL CONSTRAINT PK_MigrationState PRIMARY KEY
                             CONSTRAINT CK_MigrationState_OneRow CHECK (StateId = 1),
    Phase            TINYINT NOT NULL CONSTRAINT CK_MigrationState_Phase CHECK (Phase BETWEEN 0 AND 5),
    DualWritePercent TINYINT NOT NULL CONSTRAINT CK_MigrationState_Percent CHECK (DualWritePercent BETWEEN 0 AND 100),
    ExperimentOn     BIT     NOT NULL
);
INSERT dbo.MigrationState (StateId, Phase, DualWritePercent, ExperimentOn) VALUES (1, 0, 0, 0);
GO

-- Reads where the old store and the new store disagreed while the experiment ran.
CREATE TABLE dbo.ExperimentMismatches (
    MismatchId    BIGINT IDENTITY(1, 1) NOT NULL CONSTRAINT PK_ExperimentMismatches PRIMARY KEY,
    CustomerId    INT           NOT NULL,
    ControlJson   NVARCHAR(MAX) NULL,
    CandidateJson NVARCHAR(MAX) NULL,
    RecordedAt    DATETIME2(3)  NOT NULL CONSTRAINT DF_ExperimentMismatches_RecordedAt DEFAULT SYSUTCDATETIME()
);
GO

-- Turns one customer's JSON array into rows. Everything that reads the old store goes through this.
CREATE FUNCTION dbo.OldSubscriptionRows (@SubscriptionsJson NVARCHAR(MAX))
RETURNS TABLE
AS
RETURN
    SELECT s.SubscriptionId, s.PlanCode, s.Status, s.CreatedAt
    FROM OPENJSON(@SubscriptionsJson)
        WITH (
            SubscriptionId UNIQUEIDENTIFIER '$.id',
            PlanCode       VARCHAR(20)      '$.plan',
            Status         VARCHAR(20)      '$.status',
            CreatedAt      DATETIME2(3)     '$.created_at'
        ) AS s;
GO

-- Every subscription that differs between the two stores. Empty means they agree.
CREATE VIEW dbo.StoreDifferences
AS
WITH OldRows AS (
    SELECT c.CustomerId, s.SubscriptionId, s.PlanCode, s.Status
    FROM dbo.Customers AS c
    CROSS APPLY dbo.OldSubscriptionRows(c.SubscriptionsJson) AS s
)
SELECT COALESCE(o.CustomerId, n.CustomerId)         AS CustomerId,
       COALESCE(o.SubscriptionId, n.SubscriptionId) AS SubscriptionId,
       o.Status                                     AS OldStatus,
       n.Status                                     AS NewStatus
FROM OldRows AS o
FULL JOIN dbo.Subscriptions AS n ON n.SubscriptionId = o.SubscriptionId
WHERE o.SubscriptionId IS NULL
   OR n.SubscriptionId IS NULL
   OR o.PlanCode <> n.PlanCode
   OR o.Status <> n.Status;
GO

-- Copies the next batch of customers that haven't been copied yet. Locks each customer row first,
-- so a request writing to the same customer waits (or makes the backfill wait) instead of racing it.
CREATE PROCEDURE dbo.BackfillBatch
    @AfterCustomerId INT,
    @BatchSize       INT,
    @LastCustomerId  INT OUTPUT,
    @CopiedCnt       INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Batch TABLE (CustomerId INT NOT NULL PRIMARY KEY);

    BEGIN TRANSACTION;

    INSERT @Batch (CustomerId)
    SELECT TOP (@BatchSize) c.CustomerId
    FROM dbo.Customers AS c WITH (UPDLOCK, ROWLOCK)
    WHERE c.CustomerId > @AfterCustomerId
      AND c.MigratedAt IS NULL
    ORDER BY c.CustomerId;

    INSERT dbo.Subscriptions (SubscriptionId, CustomerId, PlanCode, Status, CreatedAt)
    SELECT s.SubscriptionId, c.CustomerId, s.PlanCode, s.Status, s.CreatedAt
    FROM @Batch AS b
    JOIN dbo.Customers AS c ON c.CustomerId = b.CustomerId
    CROSS APPLY dbo.OldSubscriptionRows(c.SubscriptionsJson) AS s
    WHERE NOT EXISTS (SELECT 1 FROM dbo.Subscriptions AS t WHERE t.SubscriptionId = s.SubscriptionId);

    SET @CopiedCnt = @@ROWCOUNT;

    UPDATE c
    SET MigratedAt = SYSUTCDATETIME()
    FROM dbo.Customers AS c
    JOIN @Batch AS b ON b.CustomerId = c.CustomerId;

    SELECT @LastCustomerId = MAX(CustomerId) FROM @Batch;

    COMMIT TRANSACTION;
END;
GO

-- Empties the old JSON column for the next batch of customers (phase 4).
CREATE PROCEDURE dbo.EmptyOldBatch
    @BatchSize  INT,
    @EmptiedCnt INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE TOP (@BatchSize) dbo.Customers
    SET SubscriptionsJson = NULL
    WHERE SubscriptionsJson IS NOT NULL;

    SET @EmptiedCnt = @@ROWCOUNT;
END;
GO
