<?php
declare(strict_types=1);
namespace Sandstorm\EventStore\LaravelAdapter;

use DateTimeImmutable;
use Illuminate\Database\Connection as DbConnection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Builder as SchemaBuilder;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Exception\ConcurrencyException;
use Neos\EventStore\Helper\BatchEventStream;
use Neos\EventStore\Model\Event;
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
use RuntimeException;
use Illuminate\Database\QueryException;

final class LaravelEventStore implements EventStoreInterface
{
    private readonly ClockInterface $clock;

    public function __construct(
        private readonly DbConnection $connection,
        private readonly string       $eventTableName,
        ?ClockInterface               $clock = null
    ) {
        $this->clock = $clock ?? new class implements ClockInterface {
            public function now(): DateTimeImmutable
            {
                return new DateTimeImmutable();
            }
        };
    }

    public function load(VirtualStreamName|StreamName $streamName, ?EventStreamFilter $filter = null): EventStreamInterface
    {
        $this->reconnectDatabaseConnection();

        /** @var \Illuminate\Database\Query\Builder $query */
        $query = $this->connection
            ->table($this->eventTableName)
            ->orderBy('sequencenumber', 'asc');

        $query = match ($streamName::class) {
            StreamName::class => $query->where('stream', $streamName->value),
            VirtualStreamName::class => match ($streamName->type) {
                VirtualStreamType::ALL => $query,
                VirtualStreamType::CATEGORY => $query->where('stream', 'like', $streamName->value . '%'),
                VirtualStreamType::CORRELATION_ID => $query->where('correlationid', $streamName->value),
            },
        };

        if ($filter !== null && $filter->eventTypes !== null) {
            $query->whereIn('type', $filter->eventTypes->toStringArray());
        }

        return BatchEventStream::create(
            LaravelEventStream::create($query),
            100
        );
    }

    public function commit(StreamName $streamName, Event|Events $events, ExpectedVersion $expectedVersion): CommitResult
    {
        if ($events instanceof Event) {
            $events = Events::fromArray([$events]);
        }

        # Exponential backoff: initial interval = 5ms and 8 retry attempts = max 1275ms (= 1,275 seconds)
        # @see http://backoffcalculator.com/?attempts=8&rate=2&interval=5

        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 8;
        $retryAttempt = 0;

        while (true) {
            $this->reconnectDatabaseConnection();

            if ($this->connection->transactionLevel() > 0) {
                throw new RuntimeException(
                    'A transaction is active already, can\'t commit events!',
                    1547829131
                );
            }

            $this->connection->beginTransaction();
            try {
                $maybeVersion = $this->getStreamVersion($streamName);
                $expectedVersion->verifyVersion($maybeVersion);

                $version = $maybeVersion->isNothing()
                    ? Version::first()
                    : $maybeVersion->unwrap()->next();

                $lastCommittedVersion = $version;
                foreach ($events as $event) {
                    $this->commitEvent($streamName, $event, $version);
                    $lastCommittedVersion = $version;
                    $version = $version->next();
                }
                $lastInsertId = $this->connection->getPdo()->lastInsertId();
                if (!is_numeric($lastInsertId)) {
                    throw new RuntimeException(
                        sprintf(
                            'Expected last insert id to be numeric, but it is: %s',
                            get_debug_type($lastInsertId)
                        ),
                        1651749706
                    );
                }

                $this->connection->commit();
                return new CommitResult(
                    $lastCommittedVersion,
                    SequenceNumber::fromInteger((int)$lastInsertId)
                );
            } catch (QueryException $e) {
                $this->connection->rollBack();

                // pull out SQLSTATE and driver error code
                $errorInfo = $e->errorInfo;
                $sqlState = $errorInfo[0] ?? null;
                $driverCode = $errorInfo[1] ?? null;

                // true for unique‐constraint (optimistic concurrency) or deadlock/lock‐timeout
                $isConcurrency = in_array($sqlState, ['23000', '40001', '40P01'], true)
                    || in_array($driverCode, [1213, 1205], true);

                if (!$isConcurrency) {
                    // not a concurrency error → rethrow immediately
                    throw $e;
                }

                // only here do we apply retries
                if ($retryAttempt >= $maxRetryAttempts) {
                    throw new ConcurrencyException(
                        sprintf('Failed after %d retry attempts', $retryAttempt),
                        (int)$e->getCode(),
                        $e
                    );
                }
                usleep((int)($retryWaitInterval * 1e6));
                $retryAttempt++;
                $retryWaitInterval *= 2;
                continue;
            } catch (ConcurrencyException|\JsonException $e) {
                $this->connection->rollBack();
                throw $e;
            }
        }
    }

    public function deleteStream(StreamName $streamName): void
    {
        $this->connection
            ->table($this->eventTableName)
            ->where('stream', $streamName->value)
            ->delete();
    }

    public function status(): Status
    {
        try {
            $this->connection->getPdo();
        } catch (\Exception $e) {
            return Status::error(sprintf('Failed to connect to database: %s', $e->getMessage()));
        }

        /** @var SchemaBuilder $schema */
        $schema = $this->connection->getSchemaBuilder();
        if (!$schema->hasTable($this->eventTableName)) {
            return Status::setupRequired(
                sprintf('Table `%s` does not exist.', $this->eventTableName)
            );
        }

        return Status::ok();
    }

    public function setup(): void
    {
        /** @var SchemaBuilder $schema */
        $schema = $this->connection->getSchemaBuilder();
        if (!$schema->hasTable($this->eventTableName)) {
            $schema->create($this->eventTableName, function (Blueprint $table) {
                $table->unsignedBigInteger('sequencenumber', true);
                $table->string('stream', StreamName::MAX_LENGTH)->charset('ascii');
                $table->unsignedBigInteger('version');
                $table->string('type', Event\EventType::MAX_LENGTH)->charset('ascii');
                $table->text('payload');
                $table->json('metadata')->nullable();
                $table->char('id', Event\EventId::MAX_LENGTH);
                $table->char('causationid', Event\CausationId::MAX_LENGTH)->nullable();
                $table->char('correlationid', Event\CorrelationId::MAX_LENGTH)->nullable();
                $table->timestamp('recordedat');

                $table->primary('sequencenumber');
                $table->unique('id');
                $table->unique(['stream', 'version']);
                $table->index('correlationid');
            });
        }
    }

    private function getStreamVersion(StreamName $streamName): MaybeVersion
    {
        $version = $this->connection
            ->table($this->eventTableName)
            ->where('stream', $streamName->value)
            ->max('version');

        return MaybeVersion::fromVersionOrNull(
            is_numeric($version)
                ? Version::fromInteger((int)$version)
                : null
        );
    }

    private function commitEvent(StreamName $streamName, Event $event, Version $version): void
    {
        $this->connection
            ->table($this->eventTableName)
            ->insert([
                'id' => $event->id->value,
                'stream' => $streamName->value,
                'version' => $version->value,
                'type' => $event->type->value,
                'payload' => $event->data->value,
                'metadata' => $event->metadata?->toJson(),
                'causationid' => $event->causationId?->value,
                'correlationid' => $event->correlationId?->value,
                'recordedat' => $this->clock->now(),
            ]);
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->connection->selectOne('SELECT 1');
        } catch (\Throwable $_) {
            $this->connection->disconnect();
            $this->connection->reconnect();
        }
    }
}
