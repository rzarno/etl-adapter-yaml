<?php
declare(strict_types=1);

namespace Flow\ETL\Adapter\YAML;

use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;

use Flow\ETL\Row\Factory\NativeEntryFactory;
use function Flow\ETL\DSL\array_to_rows;

final class YAMLExtractor implements Extractor
{
    public function __construct(
        private readonly Path $uri,
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->fs()->scan($this->uri, $context->partitionFilter()) as $path) {
            $stream = $context->streams()->fs()->open($path, Mode::READ);

            /** @var array<Row> $rowData */
            $rows = yaml_parse_file($stream->path()->path());

            if ([] !== $rows) {
                yield array_to_rows($rows, $this->entryFactory);
            }

            $stream->close();
        }
    }
}
