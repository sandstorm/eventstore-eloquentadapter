<?php
declare(strict_types=1);

namespace Sandstorm\EventStore\LaravelAdapter\Tests\Integration;

use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\QueryException;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Model\EventStore\StatusType;
use Neos\EventStore\Tests\Integration\AbstractEventStoreTestBase;
use PHPUnit\Framework\Attributes\CoversClass;
use Sandstorm\EventStore\LaravelAdapter\LaravelEventStore;

#[CoversClass(LaravelEventStore::class)]
final class LaravelEventStoreTest extends AbstractEventStoreTestBase
{
    private static ?Capsule $capsule = null;

    protected static function createEventStore(): EventStoreInterface
    {
        return new LaravelEventStore(self::connection(), self::eventTableName());
    }

    protected static function resetEventStore(): void
    {
        $schema = self::connection()->getSchemaBuilder();
        if ($schema->hasTable(self::eventTableName())) {
            $schema->drop(self::eventTableName());
        }
    }

    public static function connection(): Connection
    {
        if (self::$capsule === null) {
            $capsule = new Capsule();
            $capsule->addConnection([
                'driver'   => 'sqlite',
                'database' => __DIR__ . '/../events_test.sqlite',
                'prefix'   => '',
            ], 'default');
            $capsule->setAsGlobal();
            $capsule->bootEloquent();
            self::$capsule = $capsule;
        }
        return self::$capsule->getConnection();
    }

    public static function eventTableName(): string
    {
        return 'events_test';
    }

    public function test_setup_throws_exception_if_database_connection_fails(): void
    {
        $bad = new Capsule();
        $bad->addConnection([
            'driver'   => 'mysql',
            'host'     => 'invalid-host',
            'database' => 'does_not_exist',
            'username' => 'foo',
            'password' => 'bar',
        ], 'bad');
        $bad->setAsGlobal();
        $bad->bootEloquent();
        $conn = $bad->getConnection('bad');

        $eventStore = new LaravelEventStore($conn, self::eventTableName());
        $this->expectException(QueryException::class);
        $eventStore->setup();
    }

    public function test_status_returns_error_status_if_database_connection_fails(): void
    {
        $bad = new Capsule();
        $bad->addConnection([
            'driver'   => 'mysql',
            'host'     => 'invalid-host',
            'database' => 'does_not_exist',
            'username' => 'foo',
            'password' => 'bar',
        ], 'bad');
        $bad->setAsGlobal();
        $bad->bootEloquent();
        $conn = $bad->getConnection('bad');

        $eventStore = new LaravelEventStore($conn, self::eventTableName());
        $this->assertSame(StatusType::ERROR, $eventStore->status()->type);
    }

    public function test_status_returns_setup_required_status_if_event_table_is_missing(): void
    {
        $conn = self::connection();
        $eventStore = new LaravelEventStore($conn, self::eventTableName());
        $this->assertSame(StatusType::SETUP_REQUIRED, $eventStore->status()->type);
    }

    public function test_status_returns_setup_required_status_if_event_table_requires_update(): void
    {
        $conn = self::connection();
        $eventStore = new LaravelEventStore($conn, self::eventTableName());
        $eventStore->setup();

        // Rename one column to simulate a schema drift
        $conn->getSchemaBuilder()->table(self::eventTableName(), function (Blueprint $table) {
            $table->renameColumn('metadata', 'metadata_renamed');
        });

        $this->assertSame(StatusType::SETUP_REQUIRED, $eventStore->status()->type);
    }

    public function test_status_returns_ok_status_if_event_table_is_up_to_date(): void
    {
        $conn = self::connection();
        $eventStore = new LaravelEventStore($conn, self::eventTableName());
        $eventStore->setup();
        $this->assertSame(StatusType::OK, $eventStore->status()->type);
    }
}
