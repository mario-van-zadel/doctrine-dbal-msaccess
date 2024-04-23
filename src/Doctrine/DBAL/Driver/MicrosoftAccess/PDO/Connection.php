<?php
declare(strict_types=1);

namespace ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess\PDO;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\ParameterType;
use PDO;
use PDOStatement;
use ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess\Result;
use ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess\Statement;

final class Connection implements ConnectionInterface
{
    private readonly PDO $pdo;

    private ?bool $transactionsSupport = null;
    private ?string $charsetToEncoding = null;

    /**
     * {@inheritdoc}
     */
    public function __construct($dsn, $user = null, $password = null, ?array $options = null)
    {
        $this->pdo = new PDO($dsn, (string)$user, (string)$password, (array)$options);
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->charsetToEncoding = \array_key_exists('charset', $options)
            ? $options['charset']
            : null;
    }

    public function getServerVersion(): string
    {
        return PDO::getAttribute(PDO::ATTR_SERVER_VERSION);
    }

    /**
     * {@inheritdoc}
     */
    public function quote(string $value): string
    {
        $val = $this->pdo->quote($value);

        // Fix for a driver version terminating all values with null byte
        if (false !== \strpos($val, "\0")) {
            $val = \substr($val, 0, -1);
        }

        return $val;
    }

    /**
     * {@inheritdoc}
     */
    public function exec(string $sql): int
    {
        try {
            $result = $this->pdo->exec($sql);

            assert($result !== false);

            return $result;
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function lastInsertId($name = null): string
    {
        return '0';
    }

    public function requiresQueryForServerVersion(): bool
    {
        return false;
    }

    public function beginTransaction(): void
    {
        $this->transactionsSupported()
            ? $this->pdo->beginTransaction()
            : $this->exec('BEGIN TRANSACTION');
    }

    public function commit(): void
    {
        $this->transactionsSupported()
            ? $this->pdo->commit()
            : $this->exec('COMMIT TRANSACTION');
    }

    public function rollback(): void
    {
        $this->transactionsSupported()
            ? $this->pdo->rollback()
            : $this->exec('ROLLBACK TRANSACTION');
    }

    /**
     * {@inheritdoc}
     */
    public function query(...$args): Result
    {
        $pdoStatement = $this->pdo->query(...$args);

        \assert($pdoStatement instanceof PDOStatement);

        $statement = new Statement($pdoStatement);
        $statement->setCharsetToEncoding($this->charsetToEncoding);

        return new Result($statement);
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Statement
    {
        $pdoStatement = $this->pdo->prepare($sql);

        \assert($pdoStatement instanceof PDOStatement);

        $statement = new Statement($pdoStatement);
        $statement->setCharsetToEncoding($this->charsetToEncoding);

        return $statement;
    }

    private function transactionsSupported(): bool
    {
        if (null !== $this->transactionsSupport) {
            return $this->transactionsSupport;
        }

        try {
            $this->pdo->beginTransaction();

            $this->pdo->commit();

            $this->transactionsSupport = true;
        } catch (\PDOException $e) {
            $this->transactionsSupport = false;
        }

        return $this->transactionsSupport;
    }

    /**
     * {@inheritdoc}
     */
    public function getNativeConnection()
    {
        return $this->pdo;
    }
}
