# Laravel DB adapter for the `neos/eventstore` package

Database Adapter implementation for the [neos/eventstore](https://github.com/neos/eventstore) package.

> **Note**
> Currently this package supports MySQL (including MariaDB), PostgreSQL and SQLite.

## Usage

Install via [composer](https://getcomposer.org):

```shell
composer require sandstorm/eventstore-laraveladapter
```

### Create an instance

To create a `LaravelEventStore`, an instance of `\Illuminate\Database\Connection` is required.
This can be obtained from a given DSN for example:

With that, an Event Store instance can be created:

```php
use Sandstorm\EventStore\LaravelAdapter\LaravelEventStore;

$eventTableName = 'some_namespace_events';
$eventStore = new LaravelEventStore($connection, $eventTableName);
```

See [README](https://github.com/neos/eventstore/blob/main/README.md#usage) of the `neos/eventstore` package for details on how to write and read events.

## Contribution

Contributions in the form of [issues](https://github.com/neos/eventstore-doctrineadapter/issues), [pull requests](https://github.com/neos/eventstore-doctrineadapter/pulls) or [discussions](https://github.com/neos/eventstore-doctrineadapter/discussions) are highly appreciated

## Running Tests

```bash
# in this directory:
composer install
```

## License

See [LICENSE](./LICENSE)
