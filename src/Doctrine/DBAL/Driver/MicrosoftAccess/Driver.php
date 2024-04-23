<?php
declare(strict_types=1);

namespace ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\ServerVersionProvider;
use ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess\ODBC\Connection as ODBCConnection;
use ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess\PDO\Connection as PDOConnection;
use ZoiloMora\Doctrine\DBAL\Platforms\MicrosoftAccessPlatform;
use ZoiloMora\Doctrine\DBAL\Schema\MicrosoftAccessSchemaManager;

final class Driver implements \Doctrine\DBAL\Driver
{
    protected ?ODBCConnection $odbcConnection = null;

    public function connect(array $params): \Doctrine\DBAL\Driver\Connection {
        $driverOptions = $params['driverOptions'] ?? [];

        $this->assertRequiredDriverOptions($driverOptions);

        $conn = new PDOConnection(
            $this->constructPdoDsn($driverOptions),
            null,
            null,
            $driverOptions,
        );

        $this->odbcConnection = new ODBCConnection(
            $this->constructOdbcDsn($driverOptions),
        );

        return $conn;
    }

    public function getName(): string
    {
        return 'pdo_msaccess';
    }

    public function getDatabase(Connection $conn): string
    {
        return 'unknown';
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
    {
        return new MicrosoftAccessPlatform();
    }

    public function getExceptionConverter(): ExceptionConverterInterface
    {
        return new ExceptionConverter();
    }

    public function getSchemaManager(Connection $conn): AbstractSchemaManager
    {
        return new MicrosoftAccessSchemaManager($conn, $this->odbcConnection);
    }

    private function assertRequiredDriverOptions(array $driverOptions): void
    {
        if (false === \array_key_exists('dsn', $driverOptions)) {
            throw new Exception\InvalidArgumentException("The driver option 'dsn' is mandatory");
        }
    }

    protected function constructPdoDsn(array $driverOptions): string
    {
        return 'odbc:' . $this->getDsn($driverOptions);
    }

    protected function constructOdbcDsn(array $driverOptions): string
    {
        return $this->getDsn($driverOptions);
    }

    private function getDsn(array $driverOptions): string
    {
        return $driverOptions['dsn'];
    }
}
