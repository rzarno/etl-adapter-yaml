<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\YAML;

use Flow\ETL\Filesystem\Path;
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

        $this->write($rows, $context, []);
    }

    public function write(Rows $nextRows, FlowContext $context) : void
    {
        $rowsArray = [];
        foreach ($nextRows as $row) {
            $rowsArray[] = $row->toArray();
        }
        yaml_emit_file(
            filename: $this->path->path(),
            data: $rowsArray,
        );
    }
}
