<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\YAML\YAMLExtractor;
use Flow\ETL\Adapter\YAML\YAMLLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;

class YAML
{
    /**
     * @param array<Path|string>|Path|string $uri
     * @param EntryFactory $entry_factory
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return Extractor
     */
    final public static function from(
        string|Path|array $uri,
        EntryFactory $entry_factory = new NativeEntryFactory()
    ) : Extractor {
        if (\is_array($uri)) {
            $extractors = [];

            foreach ($uri as $file_uri) {
                $extractors[] = new YAMLExtractor(
                    \is_string($file_uri) ? Path::realpath($file_uri) : $file_uri,
                    $entry_factory
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new YAMLExtractor(
            \is_string($uri) ? Path::realpath($uri) : $uri,
            $entry_factory
        );
    }

    /**
     * @param Path|string $uri
     *
     * @return Loader
     */
    final public static function to(
        string|Path $uri
    ) : Loader {
        return new YAMLLoader(
            \is_string($uri) ? Path::realpath($uri) : $uri,
        );
    }
}
