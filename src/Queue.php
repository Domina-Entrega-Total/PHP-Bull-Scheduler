<?php

/*
 * This file is part of the PHP-Bull-Scheduler package.
 *
 * (c) DominaEntregaTotal <jorge.carrillo@domina.com.co>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace DominaEntregaTotal\BullScheduler;

use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\Exception\UnsatisfiedDependencyException;
use Predis\Client as Redis;
use vierbergenlars\SemVer\version;

class Queue {
  use LoggerAwareTrait;

  private $MINIMUM_REDIS_VERSION = '2.8.18';

  private $prefix = 'bull';

  private $name;

  private $token;

  private $keyPrefix;

  /** @var Predis\Client $redis Predis client */
  private $redis;

  public function __construct($name, $url = null, $opts = array()) {
    // Inherited from LoggerAwareTrait
    $this->setLogger(new NullLogger());

    // Prepare options
    $opts = (is_array($opts) ? $opts : array($opts));
    if (is_string($url) || $url instanceof Redis) {
      $opts['redis'] = $url;
    } else {
      $opts = (is_array($url) ? $url : array($url));
    }

    // Set queue name and token
    $this->name = $name;
    try {
      $uuid = Uuid::uuid4();
      $this->token = $uuid->toString();
    } catch (UnsatisfiedDependencyException $e) {
      $msg = sprintf("Unable to generate UUID: %s\n", $e->getMessage());
      $this->logger->error($msg);
      die($msg);
    }

    // Define prefixes
    if (isset($opts['prefix']))
      $this->prefix = $opts['prefix'];
    $this->keyPrefix = sprintf('%s:%s:', $this->prefix, $this->name);
    $this->logger->debug('Using key prefix: '.$this->keyPrefix);

    // Define Redis client
    if (isset($opts['redis']) && $opts['redis'] instanceof Redis) {
      $this->redis = ($opts['redis'] instanceof Redis ? $opts['redis'] : new Redis($opts['redis']));
    } else {
      $this->redis = new Redis();
    }

    // Ensure minimum Redis version is met
    $version = $this->redis_version($this->redis->info());
    if (!version::gte($version, $this->MINIMUM_REDIS_VERSION))
      throw new \RuntimeException(sprintf('Minimum Redis version (%s) not met. Reported Redis version: %s', $this->MINIMUM_REDIS_VERSION, $version));

    // Define addJob Lua script
    $this->redis->getProfile()->defineCommand('addjob', 'DominaEntregaTotal\BullScheduler\RedisCommand\AddJob');
  }

  public function add($name, $data = [], $opts = []) {
    // Adjust parameters if necessary
    if (!is_string($name)) {
        $opts = is_array($data) ? $data : [$data];
        $data = $name;
        $name = '__default__';
    }
    $opts = is_array($opts) ? $opts : [$opts];

    // Initial variable configuration
    $timestamp = intval(str_replace('.', '', microtime(true)));
    $delay = isset($opts['delay']) ? intval($opts['delay']) : 0;

    // Defining default values
    $defaults = [
        'attempts'       => 1,
        'timestamp'      => $timestamp,
        'delay'          => $delay,
        'priority'       => 0,
        'removeOnFail'   => false,
        'removeOnComplete' => false,
        'stackTraceLimit' => 10,
        'jobId'          => null,
        'lifo'           => false,
    ];

    // Combining default options with provided options
    $options = array_merge($defaults, $opts);

    return $this->redis->addjob(
        $this->keyPrefix . 'wait',
        $this->keyPrefix . 'paused',
        $this->keyPrefix . 'meta-paused',
        $this->keyPrefix . 'id',
        $this->keyPrefix . 'delayed',
        $this->keyPrefix . 'priority',
        $this->keyPrefix,
        $options['jobId'] ?? '',
        $name,
        json_encode($data),
        json_encode($options),
        $timestamp,
        $delay,
        ($delay > 0) ? $timestamp + $delay : 0,
        intval($options['priority']),
        $options['lifo'] ? 'RPUSH' : 'LPUSH',
        $this->token
    );
  }

  public function getRedisClient() {
    return $this->redis;
  }

  private function redis_version($info) {
    if (isset($info['Server']['redis_version'])) {
      return $info['Server']['redis_version'];
    } elseif (isset($info['redis_version'])) {
      return $info['redis_version'];
    } else {
      return $this->redis->getProfile()->getVersion();
    }
  }
}

// EOF
