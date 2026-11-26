// Compiled with exactOptionalPropertyTypes, which tests.ts does not enable.
import { KafkaJSDeleteTopicRecordsError, KafkaJSError } from './index'

// The admin copies each failed row as `partitionErrors.push({ partition, offset, error })`, so a row
// whose error is missing carries an explicit `error: undefined`, and the constructor keeps it.
new KafkaJSDeleteTopicRecordsError({
  topic: 't',
  partitions: [
    { partition: 0, offset: '1', error: undefined },
    { partition: 1, offset: '2', error: null },
    { partition: 2, offset: '3', error: new KafkaJSError('x') },
    { partition: 3, offset: undefined },
  ],
})
