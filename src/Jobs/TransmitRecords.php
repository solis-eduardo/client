<?php

namespace Laraowl\Client\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Laraowl\Client\Core;
use Laraowl\Client\State\CommandState;
use Laraowl\Client\State\RequestState;

/**
 * Sends an already-bufferred batch of records to the Laraowl server from a
 * queue worker instead of the original request/command, mirroring how
 * laravel/telescope defers `ProcessPendingUpdates` to the queue.
 *
 * @internal
 */
final class TransmitRecords implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * @param  array<int, array<mixed>>  $records
     */
    public function __construct(
        private array $records,
    ) {
        //
    }

    /**
     * @param  Core<RequestState|CommandState>  $core
     */
    public function handle(Core $core): void
    {
        $core->ingest->transmitBatch($this->records);
    }
}
