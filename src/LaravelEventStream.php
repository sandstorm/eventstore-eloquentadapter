<?php
declare(strict_types=1);

namespace Sandstorm\EventStore\LaravelAdapter;

use Illuminate\Database\Connection;
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
        private Connection $connection,
        private string $table,
        private readonly ?SequenceNumber $minSeq = null,
        private readonly ?SequenceNumber $maxSeq = null,
        private readonly ?int $limit = null,
        private readonly bool $backwards = false,
    ) {}

    public static function create(Connection $connection, string $table): self
    {
        return new self($connection, $table);
    }

    public function withMinimumSequenceNumber(SequenceNumber $seq): self
    {
        if ($this->minSeq?->value === $seq->value) {
            return $this;
        }
        return new self($this->connection, $this->table, $seq, $this->maxSeq, $this->limit, $this->backwards);
    }

    public function withMaximumSequenceNumber(SequenceNumber $seq): self
    {
        if ($this->maxSeq?->value === $seq->value) {
            return $this;
        }
        return new self($this->connection, $this->table, $this->minSeq, $seq, $this->limit, $this->backwards);
    }

    public function limit(int $limit): self
    {
        if ($this->limit === $limit) {
            return $this;
        }
        return new self($this->connection, $this->table, $this->minSeq, $this->maxSeq, $limit, $this->backwards);
    }

    public function backwards(): self
    {
        if ($this->backwards) {
            return $this;
        }
        return new self($this->connection, $this->table, $this->minSeq, $this->maxSeq, $this->limit, true);
    }

    public function getIterator(): \Traversable
    {
        // build a fresh query each time
        /** @var Builder $q */
        $q = $this->connection->table($this->table);

        if ($this->minSeq !== null) {
            $q->where('sequencenumber', '>=', $this->minSeq->value);
        }
        if ($this->maxSeq !== null) {
            $q->where('sequencenumber', '<=', $this->maxSeq->value);
        }
        if ($this->limit !== null) {
            $q->limit($this->limit);
        }
        if ($this->backwards) {
            $q->orderBy('sequencenumber', 'desc');
        }

        // handle reconnect if needed
        try {
            $this->connection->selectOne('SELECT 1');
        } catch (\Throwable) {
            $this->connection->disconnect();
            $this->connection->reconnect();
        }

        foreach ($q->get() as $row) {
            /** @var \stdClass $row */
            $ts = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row->recordedat);
            if (! $ts) {
                throw new RuntimeException(
                    sprintf('Invalid recordedat "%s" for event %s', $row->recordedat, $row->id)
                );
            }

            yield new EventEnvelope(
                new Event(
                    EventId::fromString($row->id),
                    EventType::fromString($row->type),
                    EventData::fromString($row->payload),
                    $row->metadata ? EventMetadata::fromJson($row->metadata) : null,
                    $row->causationid ? CausationId::fromString($row->causationid) : null,
                    $row->correlationid ? CorrelationId::fromString($row->correlationid) : null,
                ),
                StreamName::fromString($row->stream),
                Version::fromInteger((int)$row->version),
                SequenceNumber::fromInteger((int)$row->sequencenumber),
                $ts
            );
        }
    }
}
