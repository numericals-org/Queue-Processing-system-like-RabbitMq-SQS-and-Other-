
1. Queue Manager
    - Create/Delete/Get/List queues.
2. Publish flow
    - Validate queue configuration (capacity, delay, etc.).
3. Dispatcher
    - Dispatch messages from the correct queue.
4. Delay scheduler
    - Make delayed messages eligible when their time arrives.
5. TTL cleaner
    - Remove expired messages.
6. Metadata updates
    - Keep queue statistics accurate.
7. WAL & Snapshot integration
    - Ensure all queue operations are persisted and recoverable.

```
✅ Queue Manager

✅ Publish

⬜ Ack

⬜ Nack

⬜ RegisterProducer

⬜ RegisterConsumer

------------------------

⬜ Receiver Refactor

------------------------

⬜ Dispatcher

⬜ RetryWatcher

⬜ VisibilityWatcher

⬜ RecoverInFlightMessages

------------------------

⬜ WAL Events

⬜ Snapshot

⬜ Recovery

------------------------

⬜ Testing
```