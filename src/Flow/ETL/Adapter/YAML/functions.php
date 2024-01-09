<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\YAML;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;

function from_yml(
    string|Path|array $path
) : Extractor {
    if (\is_array($path)) {
        $extractors = [];

        foreach ($path as $file_path) {
            $extractors[] = new YAMLExtractor(
                \is_string($file_path) ? Path::realpath($file_path) : $file_path
            );
        }

        return from_all(...$extractors);
    }

    return new YAMLExtractor(
        \is_string($path) ? Path::realpath($path) : $path
    );
}

function to_yml(
    string|Path $uri
) : Loader {
    return new YAMLLoader(
        \is_string($uri) ? Path::realpath($uri) : $uri
    );
}
