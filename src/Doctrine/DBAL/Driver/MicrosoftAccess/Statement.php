<?php
declare(strict_types=1);

namespace ZoiloMora\Doctrine\DBAL\Driver\MicrosoftAccess;

use Doctrine\DBAL\Driver\PDO\Exception;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use PDO;
use PDOStatement;

final class Statement implements StatementInterface
{
    private const CHARSET_FROM_ENCODING = 'Windows-1252';

    private ?string $charsetToEncoding = null;

    /** @internal The statement can be only instantiated by its driver connection. */
    public function __construct(private readonly PDOStatement $pdoStatement)
    {
    }

    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null, $driverOptions = null)
    {
        switch ($type) {
            case ParameterType::LARGE_OBJECT:
            case ParameterType::BINARY:
                if (null === $driverOptions) {
                    $driverOptions = \PDO::SQLSRV_ENCODING_BINARY;
                }

                break;
            case ParameterType::ASCII:
                $type = ParameterType::STRING;
                $length = 0;
                $driverOptions = \PDO::SQLSRV_ENCODING_SYSTEM;

                break;
        }

        $pdoType = $this->convertParamType($type);

        try {
            $this->pdoStatement->bindValue($param, $value, $pdoType);
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    public function bindValue(int|string $param, mixed $value, ParameterType $type): void
    {
        $pdoType = $this->convertParamType($type);

        try {
            $this->pdoStatement->bindValue($param, $value, $pdoType);
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    public function execute(): Result
    {
        $this->pdoStatement->execute();

        return new Result($this);
    }

    public function setCharsetToEncoding(?string $charset): void
    {
        $this->charsetToEncoding = $charset;
    }

    public function fetchOne()
    {
        return $this->convertStringEncoding(
            $this->pdoStatement->fetch(PDO::FETCH_COLUMN)
        );
    }

    public function fetchNumeric()
    {
        return $this->convertArrayEncoding(
            $this->pdoStatement->fetch(PDO::FETCH_NUM)
        );
    }

    public function fetchAssociative()
    {
        $result = $this->pdoStatement->fetch(PDO::FETCH_ASSOC);

        if (!$result) {
            return $result;
        }

        return $this->convertArrayEncoding($result);
    }

    public function fetchAllNumeric(): array
    {
        return $this->convertCollectionEncoding(
            $this->pdoStatement->fetchAll(PDO::FETCH_NUM)
        );
    }

    public function fetchFirstColumn(): array
    {
        return $this->convertArrayEncoding(
            $this->pdoStatement->fetchColumn()
        );
    }

    public function fetchAllAssociative(): array
    {
        return $this->convertCollectionEncoding(
            $this->pdoStatement->fetchAll(PDO::FETCH_ASSOC)
        );
    }

    private function convertCollectionEncoding(array $items): array
    {
        \array_walk(
            $items,
            function (&$item) {
                $item = $this->convertArrayEncoding($item);
            },
        );

        return $items;
    }

    private function convertArrayEncoding(array $items): array
    {
        foreach (\array_keys($items) as $key) {
            $items[$key] = $this->convertStringEncoding($items[$key]);
        }

        return $items;
    }

    private function convertStringEncoding(?string $value): ?string
    {
        if (null === $this->charsetToEncoding) {
            return $value;
        }

        if (null === $value) {
            return null;
        }

        return \mb_convert_encoding($value, $this->charsetToEncoding, self::CHARSET_FROM_ENCODING);
    }

    public function rowCount(): int
    {
        return $this->pdoStatement->rowCount();
    }

    public function columnCount(): int
    {
        return $this->pdoStatement->columnCount();
    }

    public function closeCursor(): bool
    {
        return $this->pdoStatement->closeCursor();
    }

    /**
     * Converts DBAL parameter type to PDO parameter type
     *
     * @psalm-return PDO::PARAM_*
     */
    private function convertParamType(ParameterType $type): int
    {
        return match ($type) {
            ParameterType::NULL => PDO::PARAM_NULL,
            ParameterType::INTEGER => PDO::PARAM_INT,
            ParameterType::STRING,
            ParameterType::ASCII => PDO::PARAM_STR,
            ParameterType::BINARY,
            ParameterType::LARGE_OBJECT => PDO::PARAM_LOB,
            ParameterType::BOOLEAN => PDO::PARAM_BOOL,
        };
    }
}
