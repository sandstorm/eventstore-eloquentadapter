<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter\Tests\Integration;

use Doctrine\DBAL\DriverManager;
use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Tests\AbstractEventStoreTestBase;

final class DoctrineEventStoreTestBase extends AbstractEventStoreTestBase
{

    protected function createEventStore(): EventStoreInterface
    {
        $connection = DriverManager::getConnection(['url' => 'sqlite:///:memory:']);
        return new DoctrineEventStore($connection, 'some_table_name');
    }
}
