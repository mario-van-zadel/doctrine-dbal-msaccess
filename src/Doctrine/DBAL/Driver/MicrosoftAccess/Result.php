<?php

declare(strict_types=1);

namespace ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use PDO;
use PDOException;

final class Result implements ResultInterface
{
    /** @internal The result can be only instantiated by its driver connection or statement. */
    public function __construct(private readonly Statement $statement)
    {
    }

    public function fetchNumeric(): array|false
    {
        return $this->statement->fetchNumeric();
    }

    public function fetchAssociative(): array|false
    {
        return $this->statement->fetchAssociative();
    }

    public function fetchOne(): mixed
    {
        return $this->statement->fetchOne();
    }

    public function fetchAllNumeric(): array
    {
        return $this->statement->fetchAllNumeric();
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAllAssociative(): array
    {
        return $this->statement->fetchAllAssociative();
    }

    /**
     * {@inheritDoc}
     */
    public function fetchFirstColumn(): array
    {
        return $this->statement->fetchFirstColumn();
    }

    public function rowCount(): int
    {
        try {
            return $this->statement->rowCount();
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    public function columnCount(): int
    {
        try {
            return $this->statement->columnCount();
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    public function free(): void
    {
        $this->statement->closeCursor();
    }

    /**
     * @psalm-param PDO::FETCH_* $mode
     *
     * @throws Exception
     */
    private function fetch(int $mode): mixed
    {
        try {
            return $this->statement->fetch($mode);
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    /**
     * @psalm-param PDO::FETCH_* $mode
     *
     * @return list<mixed>
     *
     * @throws Exception
     */
    private function fetchAll(int $mode): array
    {
        return $this->statement->fetchAll($mode);
    }
}
