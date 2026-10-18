const EARLIEST_OFFSET = -2
const LATEST_OFFSET = -1
const INT_32_MAX_VALUE = Math.pow(2, 31) - 1
// The default response-size limit: 100 MiB, franz-go's `BrokerMaxReadBytes`.
// @see https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/config.go
const DEFAULT_MAX_RESPONSE_SIZE = 104857600

module.exports = {
  EARLIEST_OFFSET,
  LATEST_OFFSET,
  INT_32_MAX_VALUE,
  DEFAULT_MAX_RESPONSE_SIZE,
}
