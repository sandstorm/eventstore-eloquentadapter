<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Neos\EventStore\DoctrineAdapter\DoctrineCheckpointStorage;
use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Tests\Integration\AbstractEventStoreConcurrencyTestBase;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(DoctrineCheckpointStorage::class)]
final class ConcurrencyTest extends AbstractEventStoreConcurrencyTestBase
{
    /**
     * @var array<string, DoctrineEventStore>
     */
    private static array $eventStoresById = [];
    private static ?Connection $connection = null;

    public static function cleanup(): void
    {
        foreach (array_keys(self::$eventStoresById) as $eventStoreId) {
            self::connection()->executeStatement('DROP TABLE ' . self::eventTableName($eventStoreId));
        }
    }

    protected static function createEventStore(string $id): EventStoreInterface
    {
        if (!isset(self::$eventStoresById[$id])) {
            $connection = self::connection();
            $eventTableName = self::eventTableName($id);
            if ($connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
                $connection->executeStatement('TRUNCATE TABLE ' . $eventTableName . ' RESTART IDENTITY');
            } elseif ($connection->getDatabasePlatform() instanceof SqlitePlatform) {
                /** @noinspection SqlWithoutWhere */
                $connection->executeStatement('DELETE FROM ' . $eventTableName);
                $connection->executeStatement('DELETE FROM sqlite_sequence WHERE name =\'' . $eventTableName . '\'');
            } else {
                $connection->executeStatement('TRUNCATE TABLE ' . $eventTableName);
            }
            echo PHP_EOL . 'Prepared tables for ' . $connection->getDatabasePlatform()::class . PHP_EOL;
            self::$eventStoresById[$id] = new DoctrineEventStore($connection, $eventTableName);
        }
        return self::$eventStoresById[$id];
    }

    private static function connection(): Connection
    {
        if (self::$connection === null) {
            $dsn = getenv('DB_DSN');
            if (!is_string($dsn)) {
                $dsn = 'sqlite:///events_test.sqlite';
            }
            self::$connection = DriverManager::getConnection(['url' => $dsn]);
        }
        return self::$connection;
    }

    private static function eventTableName(string $eventStoreId): string
    {
        return 'events_test_' . $eventStoreId;
    }
}
