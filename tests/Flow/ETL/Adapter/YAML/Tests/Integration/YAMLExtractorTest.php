<?php
declare(strict_types=1);

namespace Flow\ETL\Adapter\YAML\Tests\Integration;

use Flow\ETL\DSL\YAML;
use function Flow\ETL\Adapter\YAML\from_yml;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\YAML\Exception\WrongFileFormatException;
use Flow\ETL\Config;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class YAMLExtractorTest extends TestCase
{
    public function test_extracting_yml_file_param_names_and_row_count() : void
    {
        $path = __DIR__ . '/../Fixtures/stars.yml';

        $rows = df()
            ->read(YAML::from($path))
            ->fetch();

        $this->assertSame(
            [
                'objid',
                'ra',
                'dec',
                'u',
                'g',
                'r',
                'i',
                'z',
                'run',
                'rerun',
                'camcol',
                'field',
                'specobjid',
                'class',
                'redshift',
                'plate',
                'mjd',
                'fiberid',

            ],
            \array_keys($rows[0]->toArray())
        );

        $this->assertSame(10000, $rows->count());
    }

    public function test_extracting_csv_files_from_directory_recursively() : void
    {
        $extractor = YAML::from(
            [
                Path::realpath(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-yml.yml'),
                Path::realpath(__DIR__ . '/../Fixtures/nested/annual-enterprise-survey-2019-financial-year-provisional-yml.yml'),
            ]
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(new FlowContext(Config::default())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    [
                        'Year',
                        'Industry_aggregation_NZSIOC',
                        'Industry_code_NZSIOC',
                        'Industry_name_NZSIOC',
                        'Units',
                        'Variable_code',
                        'Variable_name',
                        'Variable_category',
                        'Value',
                        'Industry_code_ANZSIC06',
                    ],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(64890, $total);
    }

    public function test_extracting_yml_with_different_param_number() : void
    {
        $rows = df()
            ->extract(YAML::from(__DIR__ . '/../Fixtures/different_param_count.yml'))
            ->fetch();

        $this->assertSame(2, $rows->count());
    }

    public function test_loading_data_from_all_partitions() : void
    {
        $this->assertSame(
            [
                ['group' => 1, 'id' => 1, 'value' => 'a'],
                ['group' => 1, 'id' => 2, 'value' => 'b'],
                ['group' => 1, 'id' => 3, 'value' => 'c'],
                ['group' => 1, 'id' => 4, 'value' => 'd'],
                ['group' => 2, 'id' => 5, 'value' => 'e'],
                ['group' => 2, 'id' => 6, 'value' => 'f'],
                ['group' => 2, 'id' => 7, 'value' => 'g'],
                ['group' => 2, 'id' => 8, 'value' => 'h'],
            ],
            df()
                ->read(YAML::from(__DIR__ . '/../Fixtures/partitioned/group=*/*.yml'))
                ->withEntry('id', ref('id')->cast('int'))
                ->sortBy(ref('id'))
                ->fetch()
                ->toArray()
        );
    }

    public function test_loading_data_from_all_with_local_fs() : void
    {
        $this->assertSame(
            [
                ['group' => 1, 'id' => 1, 'value' => 'a'],
                ['group' => 1, 'id' => 2, 'value' => 'b'],
                ['group' => 1, 'id' => 3, 'value' => 'c'],
                ['group' => 1, 'id' => 4, 'value' => 'd'],
                ['group' => 2, 'id' => 5, 'value' => 'e'],
                ['group' => 2, 'id' => 6, 'value' => 'f'],
                ['group' => 2, 'id' => 7, 'value' => 'g'],
                ['group' => 2, 'id' => 8, 'value' => 'h'],
            ],
            (new Flow((new ConfigBuilder())->filesystem(new LocalFilesystem())))
                ->read(YAML::from(__DIR__ . '/../Fixtures/partitioned/group=*/*.yml'))
                ->withEntry('id', ref('id')->cast('int'))
                ->sortBy(ref('id'))
                ->fetch()
                ->toArray()
        );
    }

    public function test_loading_json_file_as_yml_gives_empty_set() : void
    {
        $this->expectException(WrongFileFormatException::class);
        $extractor = YAML::from(Path::realpath(__DIR__ . '/../Fixtures/not_yml.csv'));
        $generator = $extractor->extract(new FlowContext(Config::default()));

        $this->assertEmpty(\iterator_to_array($generator));
    }
}
