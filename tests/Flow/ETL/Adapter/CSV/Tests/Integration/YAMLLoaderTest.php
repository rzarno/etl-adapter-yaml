<?php
declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\DSL\YAML;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;
use function Flow\ETL\DSL\int_entry;

final class YAMLLoaderTest extends TestCase
{
    public function test_loading_yml_files_with_append_safe() : void
    {
        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.yml';

        (new Flow())
            ->process(
                new Rows(
                    Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Andromeda')),
                    Row::create(new Row\Entry\IntegerEntry('id', 2), new Row\Entry\StringEntry('name', 'Milkyway')),
                    Row::create(new Row\Entry\IntegerEntry('id', 3), new Row\Entry\StringEntry('name', 'Pegasus')),
                )
            )
            ->appendSafe()
            ->load(YAML::to($path))
            ->run();

        $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

        $this->assertStringContainsString(
            <<<'YAML'
---
id: 1
name: Andromeda
...
---
id: 2
name: Milkyway
...
---
id: 3
name: Pegasus
...
YAML,
            \file_get_contents($path . DIRECTORY_SEPARATOR . $files[0])
        );

        if (\file_exists($path . DIRECTORY_SEPARATOR . $files[0])) {
            \unlink($path . DIRECTORY_SEPARATOR . $files[0]);
        }
    }

    public function test_loading_yml_with_partitioning() : void
    {
        $path = \sys_get_temp_dir() . '/' . \str_replace('.', '', \uniqid('partitioned_', true));
        (new Flow())
            ->process(
                new Rows(
                    Row::create(int_entry('id', 1), int_entry('group', 1)),
                    Row::create(int_entry('id', 2), int_entry('group', 1)),
                    Row::create(int_entry('id', 3), int_entry('group', 2)),
                    Row::create(int_entry('id', 4), int_entry('group', 2)),
                )
            )
            ->partitionBy('group')
            ->load(YAML::to($path))
            ->run();

        $partitions = \array_values(\array_diff(\scandir($path), ['..', '.']));

        $this->assertSame(
            [
                'group=1',
                'group=2',
            ],
            $partitions
        );

        $group1 = \array_values(\array_diff(\scandir($path . DIRECTORY_SEPARATOR . 'group=1'), ['..', '.']))[0];
        $group2 = \array_values(\array_diff(\scandir($path . DIRECTORY_SEPARATOR . 'group=2'), ['..', '.']))[0];

        $this->assertStringContainsString(
            <<<'YAML'
---
id: 1
group: 1
...
---
id: 2
group: 1
...
YAML,
            \file_get_contents($path . DIRECTORY_SEPARATOR . 'group=1' . DIRECTORY_SEPARATOR . $group1)
        );

        $this->assertStringContainsString(
            <<<'YAML'
---
id: 3
group: 2
...
---
id: 4
group: 2
...
YAML,
            \file_get_contents($path . DIRECTORY_SEPARATOR . 'group=2' . DIRECTORY_SEPARATOR . $group2)
        );

        $this->cleanDirectory($path);
    }
//
//    public function test_loading_overwrite_csv() : void
//    {
//        $path = \sys_get_temp_dir() . '/' . \uniqid('flow_php_etl_csv_loader', true) . '.csv';
//
//        (new Flow())
//            ->process(
//                new Rows(
//                    Row::create(int_entry('id', 1), str_entry('name', 'Norbert')),
//                    Row::create(int_entry('id', 2), str_entry('name', 'Tomek')),
//                    Row::create(int_entry('id', 3), str_entry('name', 'Dawid')),
//                )
//            )
//            ->load(to_csv($path))
//            ->run();
//
//        (new Flow())
//            ->process(
//                new Rows(
//                    Row::create(int_entry('id', 1), str_entry('name', 'Norbert')),
//                    Row::create(int_entry('id', 2), str_entry('name', 'Tomek')),
//                    Row::create(int_entry('id', 3), str_entry('name', 'Dawid')),
//                )
//            )
//            ->saveMode(overwrite())
//            ->load(to_csv($path))
//            ->run();
//
//        $this->assertStringContainsString(
//            <<<'CSV'
//id,name
//1,Norbert
//2,Tomek
//3,Dawid
//CSV,
//            \file_get_contents($path)
//        );
//
//        if (\file_exists($path)) {
//            \unlink($path);
//        }
//    }
//
//    public function test_using_pattern_path() : void
//    {
//        $this->expectExceptionMessage("CSVLoader path can't be pattern, given: /path/*/pattern.csv");
//
//        to_csv(new Path('/path/*/pattern.csv'));
//    }
//
    /**
     * @param string $path
     */
    private function cleanDirectory(string $path) : void
    {
        if (\file_exists($path) && \is_dir($path)) {
            $files = \array_values(\array_diff(\scandir($path), ['..', '.']));

            foreach ($files as $file) {
                if (\is_file($path . DIRECTORY_SEPARATOR . $file)) {
                    $this->removeFile($path . DIRECTORY_SEPARATOR . $file);
                } else {
                    $this->cleanDirectory($path . DIRECTORY_SEPARATOR . $file);
                }
            }

            \rmdir($path);
        }
    }

    /**
     * @param string $path
     */
    private function removeFile(string $path) : void
    {
        if (\file_exists($path)) {
            \unlink($path);
        }
    }
}