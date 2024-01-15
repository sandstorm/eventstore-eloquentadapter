<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use DateTimeImmutable;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaConfig;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Exception\ConcurrencyException;
use Neos\EventStore\Helper\BatchEventStream;
use Neos\EventStore\Model\Event;
use Neos\EventStore\Model\Event\EventType;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Model\Event\StreamName;
use Neos\EventStore\Model\Event\Version;
use Neos\EventStore\Model\Events;
use Neos\EventStore\Model\EventStore\CommitResult;
use Neos\EventStore\Model\EventStore\Status;
use Neos\EventStore\Model\EventStream\EventStreamFilter;
use Neos\EventStore\Model\EventStream\EventStreamInterface;
use Neos\EventStore\Model\EventStream\ExpectedVersion;
use Neos\EventStore\Model\EventStream\MaybeVersion;
use Neos\EventStore\Model\EventStream\VirtualStreamName;
use Neos\EventStore\Model\EventStream\VirtualStreamType;
use Psr\Clock\ClockInterface;

final class DoctrineEventStore implements EventStoreInterface
{
    private readonly ClockInterface $clock;

    public function __construct(
        private readonly Connection $connection,
        private readonly string $eventTableName,
        ClockInterface $clock = null
    ) {
        $this->clock = $clock ?? new class implements ClockInterface {
            public function now(): DateTimeImmutable
            {
                return new DateTimeImmutable();
            }
        };
    }

    public function load(VirtualStreamName|StreamName $streamName, EventStreamFilter $filter = null): EventStreamInterface
    {
        $this->reconnectDatabaseConnection();
        $queryBuilder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->eventTableName)
            ->orderBy('sequencenumber', 'ASC');

        $queryBuilder = match ($streamName::class) {
            StreamName::class => $queryBuilder->andWhere('stream = :streamName')->setParameter('streamName', $streamName->value),
            VirtualStreamName::class => match ($streamName->type) {
                VirtualStreamType::ALL => $queryBuilder,
                VirtualStreamType::CATEGORY => $queryBuilder->andWhere('stream LIKE :streamNamePrefix')->setParameter('streamNamePrefix', $streamName->value . '%'),
                VirtualStreamType::CORRELATION_ID => $queryBuilder->andWhere('correlationId LIKE :correlationId')->setParameter('correlationId', $streamName->value),
            },
        };
        if ($filter !== null && $filter->eventTypes !== null) {
            $queryBuilder->andWhere('type IN (:eventTypes)')->setParameter('eventTypes', $filter->eventTypes->toStringArray(), Connection::PARAM_STR_ARRAY);
        }
        return BatchEventStream::create(DoctrineEventStream::create($queryBuilder), 100);
    }

    public function commit(StreamName $streamName, Events $events, ExpectedVersion $expectedVersion): CommitResult
    {
        # Exponential backoff: initial interval = 5ms and 8 retry attempts = max 1275ms (= 1,275 seconds)
        # @see http://backoffcalculator.com/?attempts=8&rate=2&interval=5
        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 8;
        $retryAttempt = 0;
        while (true) {
            $this->reconnectDatabaseConnection();
            if ($this->connection->getTransactionNestingLevel() > 0) {
                throw new \RuntimeException('A transaction is active already, can\'t commit events!', 1547829131);
            }
            $this->connection->beginTransaction();
            try {
                $maybeVersion = $this->getStreamVersion($streamName);
                $expectedVersion->verifyVersion($maybeVersion);
                $version = $maybeVersion->isNothing() ? Version::first() : $maybeVersion->unwrap()->next();
                $lastCommittedVersion = $version;
                foreach ($events as $event) {
                    $this->commitEvent($streamName, $event, $version);
                    $lastCommittedVersion = $version;
                    $version = $version->next();
                }
                $lastInsertId = $this->connection->lastInsertId();
                if (!is_numeric($lastInsertId)) {
                    throw new \RuntimeException(sprintf('Expected last insert id to be numeric, but it is: %s', get_debug_type($lastInsertId)), 1651749706);
                }
                $this->connection->commit();
                return new CommitResult($lastCommittedVersion, SequenceNumber::fromInteger((int)$lastInsertId));
            } catch (UniqueConstraintViolationException $exception) {
                if ($retryAttempt >= $maxRetryAttempts) {
                    $this->connection->rollBack();
                    throw new ConcurrencyException(sprintf('Failed after %d retry attempts', $retryAttempt), 1573817175, $exception);
                }
                usleep((int)($retryWaitInterval * 1E6));
                $retryAttempt++;
                $retryWaitInterval *= 2;
                $this->connection->rollBack();
                continue;
            } catch (DeadlockException | LockWaitTimeoutException $exception) {
                $this->connection->rollBack();
                throw new ConcurrencyException($exception->getMessage(), 1705330559, $exception);
            } catch (DbalException | ConcurrencyException | \JsonException $exception) {
                $this->connection->rollBack();
                throw $exception;
            }
        }
    }

    public function deleteStream(StreamName $streamName): void
    {
        $this->connection->delete($this->eventTableName, [
            'stream' => $streamName->value
        ]);
    }

    public function status(): Status
    {
        try {
            $this->connection->connect();
        } catch (DbalException $e) {
            return Status::error(sprintf('Failed to connect to database: %s', $e->getMessage()));
        }
        $requiredSqlStatements = $this->determineRequiredSqlStatements();
        if ($requiredSqlStatements !== []) {
            return Status::setupRequired(sprintf('The following SQL statement%s required: %s', count($requiredSqlStatements) !== 1 ? 's are' : ' is', implode(chr(10), $requiredSqlStatements)));
        }
        return Status::ok();
    }

    public function setup(): void
    {
        foreach ($this->determineRequiredSqlStatements() as $statement) {
            $this->connection->executeStatement($statement);
        }
    }

    /**
     * @return array<string>
     */
    private function determineRequiredSqlStatements(): array
    {
        $schemaManager = $this->connection->getSchemaManager();
        assert($schemaManager !== null);
        $platform = $this->connection->getDatabasePlatform();
        assert($platform !== null);
        if (!$schemaManager->tablesExist($this->eventTableName)) {
            return $platform->getCreateTableSQL($this->createEventStoreSchema($schemaManager)->getTable($this->eventTableName));
        }
        $tableSchema = $schemaManager->listTableDetails($this->eventTableName);
        $fromSchema = new Schema([$tableSchema], [], $schemaManager->createSchemaConfig());
        $schemaDiff = (new Comparator())->compare($fromSchema, $this->createEventStoreSchema($schemaManager));
        return $schemaDiff->toSaveSql($platform);
    }

    // ----------------------------------

    /**
     * Creates the Doctrine schema to be compared with the current db schema for migration
     *
     * @return Schema
     */
    private function createEventStoreSchema(AbstractSchemaManager $schemaManager): Schema
    {
        $table = new Table($this->eventTableName, [
            // The monotonic sequence number
            (new Column('sequencenumber', Type::getType(Types::INTEGER)))
                ->setAutoincrement(true),

            // The stream name, usually in the format "<BoundedContext>:<StreamName>"
            (new Column('stream', Type::getType(Types::STRING)))
                ->setLength(StreamName::MAX_LENGTH)
                ->setCustomSchemaOption('charset', 'ascii')
                ->setCustomSchemaOption('collation', 'ascii_general_ci'),

            // Version of the event in the respective stream
            (new Column('version', Type::getType(Types::BIGINT)))
                ->setUnsigned(true),

            // The event type, often in the format "<BoundedContext>:<EventType>"
            (new Column('type', Type::getType(Types::STRING)))
                ->setLength(EventType::MAX_LENGTH)
                ->setCustomSchemaOption('charset', 'ascii')
                ->setCustomSchemaOption('collation', 'ascii_general_ci'),

            // The event payload, usually stored as JSON
            (new Column('payload', Type::getType(Types::TEXT)))
                ->setCustomSchemaOption('collation', 'utf8mb4_unicode_520_ci'),

            // The event metadata stored as JSON
            (new Column('metadata', Type::getType(Types::TEXT)))
                ->setCustomSchemaOption('collation', 'utf8mb4_unicode_520_ci'),

            // The unique event id, usually a UUID
            (new Column('id', Type::getType(Types::BINARY)))
                ->setLength(36),

            // An optional correlation id, usually a UUID
            (new Column('correlationid', Type::getType(Types::BINARY)))
                ->setNotnull(false)
                ->setLength(36),

            // An optional causation id, usually a UUID
            (new Column('causationid', Type::getType(Types::BINARY)))
                ->setNotnull(false)
                ->setLength(36),

            // Timestamp of the event publishing
            (new Column('recordedat', Type::getType(Types::DATETIME_IMMUTABLE)))
        ]);

        $table->setPrimaryKey(['sequencenumber']);
        $table->addUniqueIndex(['id'], 'id_uniq');
        $table->addUniqueIndex(['stream', 'version'], 'stream_version_uniq');
        $table->addIndex(['correlationid']);

        $schemaConfiguration = $schemaManager->createSchemaConfig();
        $schemaConfiguration->setDefaultTableOptions(['charset' => 'utf8mb4']);
        return new Schema([$table], [], $schemaConfiguration);
    }

    /**
     * @throws DriverException
     * @throws DbalException
     */
    private function getStreamVersion(StreamName $streamName): MaybeVersion
    {
        $result = $this->connection->createQueryBuilder()
            ->select('MAX(version)')
            ->from($this->eventTableName)
            ->where('stream = :streamName')
            ->setParameter('streamName', $streamName->value)
            ->execute();
        if (!$result instanceof Result) {
            throw new \RuntimeException(sprintf('Failed to determine stream version of stream "%s"', $streamName->value), 1651153859);
        }
        $version = $result->fetchOne();
        return MaybeVersion::fromVersionOrNull(is_numeric($version) ? Version::fromInteger((int)$version) : null);
    }

    /**
     * @throws DbalException | UniqueConstraintViolationException| \JsonException
     */
    private function commitEvent(StreamName $streamName, Event $event, Version $version): void
    {
        $this->connection->insert(
            $this->eventTableName,
            [
                'id' => $event->id->value,
                'stream' => $streamName->value,
                'version' => $version->value,
                'type' => $event->type->value,
                'payload' => $event->data->value,
                'metadata' => $event->metadata->toJson(),
                'correlationid' => $event->metadata->get('correlationId'),
                'causationid' => $event->metadata->get('causationId'),
                'recordedat' => $this->clock->now(),
            ],
            [
                'version' => Types::INTEGER,
                'recordedat' => Types::DATETIME_IMMUTABLE,
            ]
        );
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->connection->fetchOne('SELECT 1');
        } catch (\Exception $_) {
            $this->connection->close();
            $this->connection->connect();
        }
    }
}
