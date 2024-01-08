<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\YAML;

use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{
 *     path: Path
 *  }>
 */
final class YAMLLoader implements Loader, Loader\FileLoader
{
    public function __construct(
        private readonly Path $path,
    ) {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("YAMLLoader path can't be pattern, given: " . $this->path->path());
        }
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
    }

    /**
     * @psalm-suppress InvalidPropertyAssignmentValue
     */
    public function closure(Rows $rows, FlowContext $context) : void
    {
        $context->streams()->close($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        if ($context->partitionEntries()->count()) {
            foreach ($rows->partitionBy(...$context->partitionEntries()->all()) as $partitionedRows) {
                $this->write($partitionedRows, $context, $partitionedRows->partitions());
            }
        } else {
            $this->write($rows, $context, []);
        }
    }

    public function write(Rows $nextRows, FlowContext $context, array $partitions) : void
    {
        foreach ($nextRows as $row) {
            $this->writeYaml(
                $row->toArray(),
                $context->streams()->open($this->path, 'yml', $context->appendSafe(), $partitions)
            );
        }
    }

    private function writeYaml(array $row, FileStream $destination)
    {
        $data = yaml_emit(data: $row);
        $data = $this->removeFirstAndLastLine($data);

        fwrite($destination->resource(), $data);
    }

    private function removeFirstAndLastLine($text) {
        $text = substr($text, strpos($text, "\n") + 1);
        return substr($text, 0, strrpos($text, "\n") - 3);
    }

}
