<?php

namespace Laraowl\Client\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Contracts\Bus\Dispatcher;
use Illuminate\Support\Facades\Facade;
use Laraowl\Client\HttpIngest;
use Laraowl\Client\Jobs\TransmitRecords;
use Laraowl\Client\RecordsBuffer;
use PHPUnit\Framework\TestCase;

use function putenv;

/**
 * Exercises HttpIngest::digest() without booting a full Laravel application.
 * Only the `config` binding and the Facade application are set up manually,
 * which is enough for the `config()`/`now()` helpers used by digest() to
 * resolve; a fake Bus dispatcher captures whatever job would have been sent
 * to the queue.
 */
final class HttpIngestQueueTest extends TestCase
{
    /** @var array<int, object> */
    public array $dispatchedJobs = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->dispatchedJobs = [];

        $container = new Container;
        Container::setInstance($container);

        $this->rebindConfig();

        // Only dispatch() is ever exercised by TransmitRecords::dispatch();
        // a stub leaves every other Dispatcher method as a harmless no-op.
        $dispatcher = $this->createStub(Dispatcher::class);
        $dispatcher->method('dispatch')
            ->willReturnCallback(function ($command) {
                $this->dispatchedJobs[] = $command;
            });
        $container->instance(Dispatcher::class, $dispatcher);

        // HttpIngest::transmitBatch() logs via the Log facade when the request
        // fails (which it always will here, given the unroutable endpoint);
        // stub it out so the facade has something to resolve.
        $container->instance('log', new class
        {
            public function error($message, array $context = []): void {}
        });

        Facade::setFacadeApplication($container);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Container::setInstance(null);
        putenv('LARAOWL_QUEUE_CONNECTION');
        putenv('LARAOWL_QUEUE');

        parent::tearDown();
    }

    public function test_it_dispatches_a_queued_job_when_a_queue_connection_is_configured(): void
    {
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        putenv('LARAOWL_QUEUE=custom-queue');
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $job = $this->dispatchedJobs[0];
        $this->assertInstanceOf(TransmitRecords::class, $job);
        $this->assertSame('redis', $job->connection);
        $this->assertSame('custom-queue', $job->queue);
    }

    public function test_it_does_not_dispatch_a_job_by_default_and_falls_back_to_synchronous_transmit(): void
    {
        // No LARAOWL_QUEUE_CONNECTION set: this is the out-of-the-box config,
        // which must keep sending synchronously even though LARAOWL_QUEUE has
        // a non-empty default queue name.
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(0, $this->dispatchedJobs);
    }

    public function test_an_empty_queue_name_falls_back_to_the_connections_default_queue(): void
    {
        // LARAOWL_QUEUE='' must behave like "not configured" (use whatever
        // queue the connection defaults to), not push jobs onto a queue
        // literally named '' that nothing listens on by default.
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        putenv('LARAOWL_QUEUE=');
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $this->assertNull($this->dispatchedJobs[0]->queue);
    }

    public function test_it_delays_the_job_when_a_queue_delay_is_configured(): void
    {
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        $this->rebindConfig();
        Container::getInstance()->make('config')->set('laraowl.queue.delay', 30);

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $this->assertNotNull($this->dispatchedJobs[0]->delay);
    }

    private function rebindConfig(): void
    {
        Container::getInstance()->instance('config', new Repository([
            'laraowl' => require __DIR__.'/../../config/laraowl.php',
        ]));
    }

    private function makeIngest(): HttpIngest
    {
        // Unroutable loopback port: the synchronous fallback path fails fast
        // (connection refused) instead of hitting the network or timing out.
        return new HttpIngest(
            endpoint: 'http://127.0.0.1:1',
            token: 'test-token',
            timeout: 0.5,
            buffer: new RecordsBuffer(10),
        );
    }
}
