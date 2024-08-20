# PHP Bull Scheduler
PHP library to schedule jobs for the NodeJS [Bull Redis queue](https://github.com/OptimalBits/bull).

## Requirements
* PHP >= 5.5.0
* PHP JSON extension
* Redis >= 2.8.18

## Installation
Install via composer:
`composer require dominaentregatotal/php-bull-scheduler`

## Usage
This library operates under the namspace `DominaEntregaTotal\BullScheduler` and uses [Predis](https://github.com/nrk/predis) under the hood.
```php
<?php

require_once 'vendor/autoload.php';

use DominaEntregaTotal\BullScheduler\Queue;

// You can specify any value for Redis that Predis considers valid for the first parameter of Predis\Client
$queue = new Queue('name-queue', 'tcp://localhost:6379');
$queue2 = new Queue('name-queue', ['redis' => ['host' => 'localhost', 'port' => 6379]]);
$queue3 = new Queue('other-queue', new Predis\Client());

$job_id = $queue->add(['data' => 'value']);

// with process name and options
$job_id = $queue->add('name-process', [
                            'key' => 'value',
                        ], [
                            'attempts'  => 3, // try 3 attemps
                            'delay'     => 1000, // delay 1 second in miliseconds
                        ]);
```

## Caveats
* This library has been tested with Bull v3.6.0. No other versions have been tested, so use at your own risk with other versions.
* This library only handles scheduling (adding new jobs) and will never handle job processing of any form, including job statuses.

## License
The code for PHP Bull Scheduler is distributed under the terms of the MIT license (see [LICENSE](LICENSE)).
