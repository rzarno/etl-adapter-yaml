# ETL Adapter: YAML

Flow PHP's Adapter YAML is a proficient library crafted to enable seamless interaction with YAML data within your ETL (
Extract, Transform, Load) workflows. This adapter is indispensable for developers aiming to effortlessly extract from or
load data into YAML formats, ensuring a smooth and reliable data transformation journey. By employing the Adapter YAML
library, developers can access a robust set of features tailored for precise YAML data handling, making complex data
transformations both manageable and efficient. The Adapter YAML library encapsulates a broad range of functionalities,
providing a streamlined API for engaging with YAML data, which is vital in modern data processing and transformation
scenarios. This library embodies Flow PHP's dedication to offering versatile and effective data processing solutions,
making it a prime choice for developers dealing with YAML data in large-scale and data-intensive projects. With Flow
PHP's Adapter YAML, managing YAML data within your ETL workflows becomes a more simplified and efficient task, perfectly
aligning with the robust and adaptable framework of the Flow PHP ecosystem.

## Installation 

``` 
composer require flow-php/etl-adapter-yaml:1.x@dev
```

## Extractor

```php
<?php

use Flow\ETL\DSL\YAML;
use Flow\ETL\Flow;

$rows = (new Flow())
    ->read(YAML::from(new LocalFile($path)))
    ->fetch();
```

## Loader 

```php 
<?php

use Flow\ETL\DSL\YAML;
use Flow\ETL\Row;
use Flow\ETL\Rows;

(new Flow())
    ->process(
        new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Norbert')),
            Row::create(new Row\Entry\IntegerEntry('id', 2), new Row\Entry\StringEntry('name', 'Tomek')),
            Row::create(new Row\Entry\IntegerEntry('id', 3), new Row\Entry\StringEntry('name', 'Dawid')),
        )
    )
    ->load(YAML::to($path, true, true))
    ->run();
```
