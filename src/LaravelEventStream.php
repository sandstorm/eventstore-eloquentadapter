<?php
declare(strict_types=1);
namespace Sandstorm\EventStore\EloquentAdapter;

use Illuminate\Database\Query\Builder;
use Neos\EventStore\Model\Event;
use Neos\EventStore\Model\Event\CausationId;
use Neos\EventStore\Model\Event\CorrelationId;
use Neos\EventStore\Model\Event\EventData;
use Neos\EventStore\Model\Event\EventId;
use Neos\EventStore\Model\Event\EventMetadata;
use Neos\EventStore\Model\EventStream\EventStreamInterface;
use Neos\EventStore\Model\Event\EventType;
use Neos\EventStore\Model\EventEnvelope;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Model\Event\StreamName;
use Neos\EventStore\Model\Event\Version;
use DateTimeImmutable;
use RuntimeException;

final class LaravelEventStream implements EventStreamInterface
{
    private function __construct(
        private Builder $queryBuilder,
        private readonly ?SequenceNumber $minimumSequenceNumber,
        private readonly ?SequenceNumber $maximumSequenceNumber,
        private readonly ?int $limit,
        private readonly bool $backwards,
    ) {
    }

    public static function create(Builder $queryBuilder): self
    {
        return new self($queryBuilder, null, null, null, false);
    }

    public function withMinimumSequenceNumber(SequenceNumber $sequenceNumber): self
    {
        if ($this->minimumSequenceNumber !== null && $sequenceNumber->value === $this->minimumSequenceNumber->value) {
            return $this;
        }
        return new self($this->queryBuilder, $sequenceNumber, $this->maximumSequenceNumber, $this->limit, $this->backwards);
    }

    public function withMaximumSequenceNumber(SequenceNumber $sequenceNumber): self
    {
        if ($this->maximumSequenceNumber !== null && $sequenceNumber->value === $this->maximumSequenceNumber->value) {
            return $this;
        }
        return new self($this->queryBuilder, $this->minimumSequenceNumber, $sequenceNumber, $this->limit, $this->backwards);
    }

    public function limit(int $limit): self
    {
        if ($limit === $this->limit) {
            return $this;
        }
        return new self($this->queryBuilder, $this->minimumSequenceNumber, $this->maximumSequenceNumber, $limit, $this->backwards);
    }

    public function backwards(): self
    {
        if ($this->backwards) {
            return $this;
        }
        return new self($this->queryBuilder, $this->minimumSequenceNumber, $this->maximumSequenceNumber, $this->limit, true);
    }

    public function getIterator(): \Traversable
    {
        $queryBuilder = clone $this->queryBuilder;

        if ($this->minimumSequenceNumber !== null) {
            $queryBuilder->where('sequencenumber', '>=', $this->minimumSequenceNumber->value);
        }
        if ($this->maximumSequenceNumber !== null) {
            $queryBuilder->where('sequencenumber', '<=', $this->maximumSequenceNumber->value);
        }
        if ($this->limit !== null) {
            $queryBuilder->limit($this->limit);
        }
        if ($this->backwards) {
            $queryBuilder->orderBy('sequencenumber', 'desc');
        }

        $this->reconnectDatabaseConnection();

        foreach ($queryBuilder->get() as $row) {
            /** @var \stdClass $row */
            $recordedAt = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row->recordedat);
            if ($recordedAt === false) {
                throw new RuntimeException(sprintf('Failed to parse "recordedat" value of "%s" in event "%s"', $row->recordedat, $row->id));
            }
            yield new EventEnvelope(
                new Event(
                    EventId::fromString($row->id),
                    EventType::fromString($row->type),
                    EventData::fromString($row->payload),
                    isset($row->metadata) ? EventMetadata::fromJson($row->metadata) : null,
                    isset($row->causationid) ? CausationId::fromString($row->causationid) : null,
                    isset($row->correlationid) ? CorrelationId::fromString($row->correlationid) : null,
                ),
                StreamName::fromString($row->stream),
                Version::fromInteger((int)$row->version),
                SequenceNumber::fromInteger((int)$row->sequencenumber),
                $recordedAt
            );
        }
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->queryBuilder
                ->getConnection()
                ->selectOne('SELECT 1');
        } catch (\Throwable $_) {
            $conn = $this->queryBuilder->getConnection();
            $conn->disconnect();
            $conn->reconnect();
        }
    }
}
