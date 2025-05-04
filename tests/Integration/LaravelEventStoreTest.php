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

class TestApplication extends Container
{
    public function basePath(string $path = ''): string
    {
        // point this at your package root (or test root) so that SQLite can write its files
        $root = __DIR__ . '/..';
        return $path ? $root . DIRECTORY_SEPARATOR . $path : $root;
    }

    public function databasePath(string $path = ''): string
    {
        // if you want SQLite files under "database/"
        $db = $this->basePath('database');
        return $path ? $db . DIRECTORY_SEPARATOR . $path : $db;
    }
}


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
        unlink(__DIR__ . '/../events_test.sqlite');
    }

    public static function connection(): Connection
    {
        if (self::$capsule === null) {

            // 1) replace the global container
            Container::setInstance(new TestApplication());

            $path = __DIR__ . '/../events_test.sqlite';
            if (!file_exists($path)) {
                touch($path);
            }

            $capsule = new Capsule();
            $capsule->addConnection([
                'driver'   => 'sqlite',
                'database' => $path,
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
