<?php

namespace Laraowl\Client;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use Illuminate\Contracts\Bus\Dispatcher;
use Illuminate\Support\Facades\Log;
use JsonException;
use Laraowl\Client\Contracts\Ingest as IngestContract;
use Laraowl\Client\Jobs\TransmitRecords;
use Throwable;

use function app;
use function config;
use function json_decode;
use function json_encode;
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
        $this->transmitBatch([$record]);
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

        /** @var array{connection?: ?string, queue?: ?string, delay?: int} $queueConfig */
        $queueConfig = config('laraowl.queue', []);
        $connection = $queueConfig['connection'] ?? null;

        if ($connection) {
            // Queued jobs are serialized with PHP's native serialize(), not
            // json_encode(). Some record fields (e.g. the request record's
            // "user" key, RequestState::$user->id()) are LazyValue instances
            // -- JsonSerializable wrappers around a Closure, meant to be
            // resolved lazily by json_encode() on the synchronous transmit
            // path (see transmitBatch()/Payload::json()). A Closure can
            // never be serialize()'d, so constructing the job with these
            // still-lazy records throws "Serialization of 'Closure' is not
            // allowed" the moment it's actually pushed onto the queue --
            // and that exception is swallowed silently by
            // Core::finishExecution(), losing the record with no error
            // anywhere. Round-tripping through JSON here resolves every
            // LazyValue (and anything else JsonSerializable) into a plain,
            // natively serializable value -- exactly what would happen
            // anyway once the batch is eventually sent over HTTP.
            try {
                $records = json_decode(
                    json_encode($records, JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE | JSON_UNESCAPED_UNICODE),
                    true,
                    flags: JSON_THROW_ON_ERROR
                );
            } catch (JsonException $e) {
                Log::error('Laraowl Ingest Error: failed to prepare records for queueing: '.$e->getMessage());

                return;
            }

            $job = (new TransmitRecords($records))
                ->onConnection($connection)
                // The inner `?? null` only covers the key being absent (per
                // the array-shape above) so PHPStan can see the offset is
                // safe; the outer `?: null` is what actually matters here --
                // it also treats an explicit LARAOWL_QUEUE='' as "not
                // configured" instead of pushing to a queue literally named
                // '', which most workers don't listen on by default.
                ->onQueue(($queueConfig['queue'] ?? null) ?: null);

            if ($delay = $queueConfig['delay'] ?? 0) {
                $job->delay(now()->addSeconds($delay));
            }

            $this->dispatchUnsampled($job);

            return;
        }

        // No queue connection configured: fall back to the synchronous
        // transmit, same as Telescope falling back to sync when
        // TELESCOPE_QUEUE is empty. LARAOWL_QUEUE only takes effect once
        // LARAOWL_QUEUE_CONNECTION opts into queueing.
        $this->transmitBatch($records);
    }

    /**
     * Push the transmit job with sampling explicitly turned off in the
     * context Laravel dehydrates into the job payload.
     *
     * Core::prepareForJob() restores the sampling flag from the payload, and
     * Compatibility::getSamplingFromContext() defaults to *true* when the key
     * is absent -- so the transmit job would otherwise run sampled. On a
     * database queue connection the driver's own SQL (the reserve/pop on the
     * way in, the delete on the way out) is captured by QuerySensor like any
     * other query, so the next Looping event would digest those records and
     * dispatch another transmit job, whose own driver SQL would do the same:
     * an endless chain of jobs with no application traffic behind it.
     * Marking the payload unsampled makes Core::finishExecution() flush()
     * that buffer instead of digest()ing it, cutting the chain at the source.
     * The TransmitRecords guards in QueuedJobSensor and JobAttemptSensor
     * remain as a second line of defence for setups where the context never
     * reaches the payload.
     */
    private function dispatchUnsampled(TransmitRecords $job): void
    {
        $cachedSampling = Compatibility::getSamplingFromContext(null);

        try {
            Compatibility::addSamplingToContext(false);

            // Dispatch explicitly through the bus instead of the fluent
            // TransmitRecords::dispatch(...) static helper: that helper
            // returns a PendingDispatch which only actually pushes the job
            // in its __destruct(), when the local variable goes out of
            // scope. That's unreliable this late in the request lifecycle
            // (digest() is invoked from the very last hook Laravel's HTTP
            // kernel runs in Kernel::terminate(), right before script
            // shutdown). Dispatching explicitly pushes immediately and
            // removes that timing dependency entirely.
            app(Dispatcher::class)->dispatch($job);
        } finally {
            if ($cachedSampling === null) {
                Compatibility::removeSamplingFromContext();
            } else {
                Compatibility::addSamplingToContext($cachedSampling);
            }
        }
    }

    /**
     * @internal Used by {@see TransmitRecords} to send an already-bufferred
     * batch from the queue worker, and by the synchronous fallback in
     * {@see digest()}.
     */
    public function transmitBatch(array $records): void
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
