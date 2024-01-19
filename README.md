# Doctrine DBAL adapter for the `neos/eventstore` package

Database Adapter implementation for the [neos/eventstore](https://github.com/neos/eventstore) package.

> **Note**
> Currently this package supports MySQL (including MariaDB), PostgreSQL and SQLite.

## Usage

Install via [composer](https://getcomposer.org):

```shell
composer require neos/eventstore-doctrineadapter
```

### Create an instance

To create a `DoctrineEventStore`, an instance of `\Doctrine\DBAL\Connection` is required.
This can be obtained from a given DSN for example:

```php
use Doctrine\DBAL\DriverManager;

$connection = DriverManager::getConnection(['url' => $dsn]);
```

See [Doctrine documentation](https://www.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/configuration.html#getting-a-connection) for more details.

With that, an Event Store instance can be created:

```php
use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;

$eventTableName = 'some_namespace_events';
$eventStore = new DoctrineEventStore($connection, $eventTableName);
```

See [README](https://github.com/neos/eventstore/blob/main/README.md#usage) of the `neos/eventstore` package for details on how to write and read events.

## Contribution

Contributions in the form of [issues](https://github.com/neos/eventstore-doctrineadapter/issues), [pull requests](https://github.com/neos/eventstore-doctrineadapter/pulls) or [discussions](https://github.com/neos/eventstore-doctrineadapter/discussions) are highly appreciated

## License

See [LICENSE](./LICENSE)