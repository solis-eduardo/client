<?php

namespace Laraowl\Client;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use Laraowl\Client\Contracts\Ingest as IngestContract;
use Laraowl\Client\Jobs\TransmitRecords;
use Throwable;

use function config;
use function now;

/**
 * @internal
 */
final class HttpIngest implements IngestContract
{
    private bool $shouldDigestWhenBufferIsFull = true;

    public function __construct(
        private string $endpoint,
        private string $token,
        private float $timeout,
        public RecordsBuffer $buffer,
        private ?string $app_url = null,
    ) {
        //
    }

    public function write(array $record): void
    {
        $this->buffer->write($record);

        if ($this->shouldDigestWhenBufferIsFull && $this->buffer->full) {
            $this->digest();
        }
    }

    public function writeNow(array $record): void
    {
        $this->transmit([$record]);
    }

    public function flush(): void
    {
        $this->buffer->flush();
    }

    public function ping(): void
    {
        // Ping could be a health check or just ignored for HTTP
    }

    public function shouldDigest(bool $bool = true): void
    {
        $this->shouldDigestWhenBufferIsFull($bool);
    }

    public function shouldDigestWhenBufferIsFull(bool $bool = true): void
    {
        $this->shouldDigestWhenBufferIsFull = $bool;
    }

    public function digest(): void
    {
        $records = $this->buffer->pullRaw();

        if (empty($records)) {
            return;
        }

        /** @var ?string $connection */
        $connection = config('laraowl.queue.connection');
        /** @var ?string $queue */
        $queue = config('laraowl.queue.queue');

        if ($connection || $queue) {
            /** @var int $delay */
            $delay = config('laraowl.queue.delay', 0);

            TransmitRecords::dispatch($records)
                ->onConnection($connection)
                ->onQueue($queue)
                ->delay(now()->addSeconds($delay));

            return;
        }

        // No queue configured: fall back to the synchronous transmit, same as
        // Telescope falling back to sync when TELESCOPE_QUEUE is empty.
        $this->transmit($records);
    }

    /**
     * @internal Used by {@see TransmitRecords} to send an already-bufferred
     * batch from the queue worker.
     */
    public function transmitBatch(array $records): void
    {
        $this->transmit($records);
    }

    private function transmit(array $records): void
    {
        $client = new Client([
            'base_uri' => $this->endpoint,
            'timeout' => $this->timeout,
        ]);

        try {
            $client->post('/api/records', [
                'headers' => [
                    'Authorization' => 'Bearer '.$this->token,
                    'Accept' => 'application/json',
                ],
                'json' => [
                    'app_url' => $this->app_url,
                    'records' => $records,
                ],
            ]);
        } catch (GuzzleException|Throwable $e) {
            \Illuminate\Support\Facades\Log::error('Laraowl Ingest Error: '.$e->getMessage()."\n".$e->getTraceAsString());
        }
    }
}
