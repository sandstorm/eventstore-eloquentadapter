<?php
declare(strict_types=1);

namespace Sandstorm\EventStore\LaravelAdapter\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\QueryException;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Model\EventStore\StatusType;
use Neos\EventStore\Tests\Integration\AbstractEventStoreTestBase;
use PHPUnit\Framework\Attributes\CoversClass;
use Sandstorm\EventStore\LaravelAdapter\LaravelEventStore;

require_once __DIR__ . '/../../vendor/autoload.php';

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
        $connection = self::connection();

        // If the table doesn’t exist, bail early
        if (!$connection->getSchemaBuilder()->hasTable(self::eventTableName())) {
            return;
        }

        $table = self::eventTableName();
        $driver = $connection->getDriverName();

        if ($driver === 'sqlite') {
            // SQLite: delete all rows & reset autoincrement
            $connection->statement('DELETE FROM ' . $table);
            $connection->statement(
                'UPDATE SQLITE_SEQUENCE SET seq = 0 WHERE name = ?',
                [ $table ]
            );
        } elseif ($driver === 'pgsql') {
            // Postgres: truncate and restart identity
            $connection->statement(
                'TRUNCATE TABLE ' . $table . ' RESTART IDENTITY'
            );
        } else {
            // MySQL, SQL Server, etc.
            $connection->statement('TRUNCATE TABLE ' . $table);
        }
    }


    public static function connection(): Connection
    {
        if (self::$capsule === null) {
            $dbDriver = getenv('DB_DRIVER');
            if ($dbDriver !== 'mysql') {
                throw new \RuntimeException('Currently only MySQL is supported for testing. Set DB_DRIVER=mysql to run tests.');
            }

            /*for SQLITE: $path = __DIR__ . '/../events_test.sqlite';
            if (!file_exists($path)) {
                echo("!!!!!!!!!! CREATING");
                touch($path);
            }*/

            $capsule = new Capsule();
            $capsule->addConnection([
                'driver'   => $dbDriver,
                'database' => getenv('DB_DATABASE') ?: 'eventstore_test',
                'host' => '127.0.0.1',
                'port' => '3309',
                'username' => getenv('DB_USER') ?: 'root',
                'password' => getenv('DB_PASSWORD') ?: 'password',
                'prefix'   => '',
            ], 'default');
            self::$capsule = $capsule;
        }
        $connection = self::$capsule->getConnection();
        //$connection->unprepared('PRAGMA journal_mode = WAL;');
        //$connection->unprepared('PRAGMA busy_timeout = 5000;');
        return $connection;
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

    // TODO: PROBLEM WITH abstract protected static function resetEventStore(): void; ??
}
