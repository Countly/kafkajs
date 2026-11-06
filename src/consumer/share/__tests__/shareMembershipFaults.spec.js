const assert = require('node:assert/strict')
const {
  acquired,
  deferred,
  flush,
  topError,
  until,
} = require('../../../../testHelpers/shareHarness')
const ConnectionPool = require('../../../network/connectionPool')
const RequestQueue = require('../../../network/requestQueue')
const Broker = require('../../../broker')
const { shareFetchV2 } = require('../../../../testHelpers/wire/expected')
const { requests } = require('../../../protocol/requests')

const {
  FaultAudit,
  FaultCase,
  NAME,
  IDS,
  ALIAS,
  H,
  J,
  STALE_CALL,
  codesOf,
  track,
  covers,
} = require('../../../../testHelpers/shareFaultAudit')

/** Complete literal corpus, wire-mutation and public-outcome sets, including duplicate deliveries. */
const OBSERVATIONS = {
  joinControl: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  retained: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  retiredThenA: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [2],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'release',
        reason: 'membership-lost',
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  unknown: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '21222324-2526-2728-292a-2b2c2d2e2f30',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '21222324-2526-2728-292a-2b2c2d2e2f30',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '21222324-2526-2728-292a-2b2c2d2e2f30',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  empty: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1, 2],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [2],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
    ],
  },
  metadataClosed: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [2],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [2],
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'release',
        reason: 'membership-lost',
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  oldHeartbeat: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'release',
        reason: 'membership-lost',
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: 'not-sent',
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: 'not-sent',
        code: null,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  publication: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1, 2],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [2],
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  metadataRetained: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  guardFence: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [2],
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'release',
        reason: 'membership-lost',
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: null,
        code: null,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
  guardZero: {
    handlers: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        offset: '9007199254740994',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        offset: '9007199254740993',
        key: '6b',
        value: '76',
        deliveryCount: 2,
      },
    ],
    mutations: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        acknowledgeTypes: [1],
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        acknowledgeTypes: [1],
      },
    ],
    settlements: [
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 1,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 2,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
        reason: null,
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'release',
        reason: 'membership-lost',
        code: null,
      },
      {
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partition: 0,
        firstOffset: '9007199254740994',
        lastOffset: '9007199254740994',
        type: 'release',
        reason: 'not-sent',
        code: null,
      },
      {
        topicId: '11121314-1516-1718-191a-1b1c1d1e1f20',
        partition: 0,
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740993',
        type: 'accept',
        reason: null,
        code: null,
      },
    ],
  },
}

/** Literal fault, phase and outcome rows; the runtime driver must not mutate this oracle. */
const rows = [
  {
    fault: 'heartbeat 14',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 14,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 15,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 16,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 25,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 110,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 35,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 30,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 29,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 81,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 42,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: 0,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'before join',
    expected: {
      observations: OBSERVATIONS.joinControl,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'current',
      nextEpoch: null,
      cause: 69,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 14',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 14,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 15,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 16,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 25,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 110,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 35,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 30,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 29,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 81,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 42,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 14',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 14,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 15,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 16,
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 25,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'rejoin',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 110,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 35,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 30,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 29,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 81,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 42,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'current',
      nextEpoch: null,
      cause: 'connection',
      finals: 0,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 14',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 14,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 15',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 15,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 16',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 16,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 25',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 25,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 110',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 110,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 35',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 35,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 30',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 30,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 29',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 29,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 81',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 81,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 42',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 42,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat disconnect',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 'connection',
      finals: 1,
    },
  },
  {
    fault: 'heartbeat timeout',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 'connection',
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 69',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'left',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'unknown assignment',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.unknown,
      action: 'assignment',
      state: 'STABLE',
      rounds: 8,
      assignment: ['A:0', 'A:1', 'A:2'],
      resolved: ['A:0', 'A:1', 'A:2', 'U:0'],
      refresh: 1,
    },
  },
  {
    fault: 'empty assignment',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.empty,
      action: 'assignment',
      state: 'STABLE',
      rounds: 8,
      assignment: [],
      released: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      changes: 1,
    },
  },
  {
    fault: 'equal assignment',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.retained,
      action: 'assignment',
      state: 'STABLE',
      rounds: 8,
      assignment: ['A:0', 'A:1', 'A:2'],
      changes: 0,
    },
  },
  {
    fault: 'heartbeat 69 while unsubscribed',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'ignored',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      member: 'new run control',
      held: 'membership-lost',
      nextEpoch: null,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'heartbeat 69 from superseded membership',
    step: 'rejoin',
    expected: {
      observations: OBSERVATIONS.retiredThenA,
      action: 'ignored',
      state: 'STABLE',
      rounds: 8,
      member: 'same',
      held: 'membership-lost',
      nextEpoch: 0,
      cause: 69,
      finals: 1,
    },
  },
  {
    fault: 'held bulk metadata',
    step: 'leave',
    expected: {
      observations: OBSERVATIONS.metadataClosed,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held per-id metadata',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.metadataClosed,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held by-name metadata',
    step: 'rejoin',
    expected: {
      observations: OBSERVATIONS.metadataClosed,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held unresolved publication',
    step: 'topic replacement',
    expected: {
      observations: OBSERVATIONS.publication,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'equal epoch old answer',
    step: 'replacement run',
    expected: {
      observations: OBSERVATIONS.oldHeartbeat,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'lower epoch answer',
    step: 'after join',
    expected: {
      observations: OBSERVATIONS.metadataRetained,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'known ids while unknown retries',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.metadataRetained,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'same target preserves backoff',
    step: 'assignment change',
    expected: {
      observations: OBSERVATIONS.metadataRetained,
      action: 'target guard',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      current: ['B:0@9007199254740993'],
      old: ['A:1@9007199254740993'],
      target: 'distinct',
      replacement: true,
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat connect',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat encode',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat queue',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'fence',
    expected: {
      observations: OBSERVATIONS.guardFence,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 1,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held heartbeat response',
    step: 'disconnect zero',
    expected: {
      observations: OBSERVATIONS.guardZero,
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 1,
      oldEffects: 0,
      closingWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
]

/**
 * Builds the row's literal assignment without inheriting the served metadata table.
 * @param {object} call Original heartbeat request.
 * @param {number[]|null} partitions Wanted partitions of the selected topic.
 * @param {object} [options] Topic, epoch and unresolved-target fields.
 * @returns {object} Successful heartbeat answer with zero throttle.
 */
const assignmentAnswer = (
  call,
  partitions,
  { unknown = false, epoch = 1, topicId = IDS.A, extra = [] } = {}
) => ({
  throttleTime: 0,
  errorCode: 0,
  errorMessage: null,
  memberId: call.fields.memberId,
  memberEpoch: epoch,
  heartbeatIntervalMs: 50,
  assignment: {
    topicPartitions: [
      ...(partitions ? [{ topicId, partitions }] : []),
      ...(unknown ? [{ topicId: IDS.U, partitions: [0] }] : []),
      ...extra,
    ],
  },
})

const FAULT_CODE = /^heartbeat (\d+)$/
const STEADY = ({ fields }) => fields.memberEpoch > 0
const LEAVE = ({ fields }) => fields.memberEpoch === -1
const A0_H = `A:0@${H}`
const A1_H = `A:1@${H}`

/**
 * Drives one literal membership row through the public share consumer over the scripted cluster.
 * The row's `expected.action` selects the family: a heartbeat fault at a phase, an assignment cell,
 * a held metadata-flight cell or a held write-boundary cell.
 */
class MembershipFaultCase extends FaultCase {
  /**
   * @param {object} row The literal oracle row.
   * @returns {MembershipFaultCase} A case with its fault parsed from the row.
   */
  constructor(row) {
    super(row)
    const coded = FAULT_CODE.exec(row.fault)
    this.code = coded ? Number(coded[1]) : null
    this.transport = /disconnect$|timeout$/.exec(row.fault)?.[0] ?? null
    this.family = { assignment: 'assignment', 'target guard': 'target', guard: 'guard' }[
      row.expected.action
    ]
    this.family ??= 'fault'
    this.requestTimeout = 1000
  }

  /** @returns {Promise<void>} Creates the client with the row's request-timeout configuration. */
  async startClient() {
    const { enforce } = this.row.expected
    await this.start({
      requestTimeout: this.transport === 'timeout' || this.family === 'guard' ? 1000 : 30000,
      retry: { initialRetryTime: 250, factor: 0, multiplier: 1, maxRetryTime: 250 },
      enforce: enforce ?? true,
    })
  }

  /**
   * Supplies the separately declared single-record B/U controls; A keeps its H/J corpus.
   * @param {object} fields Independently decoded fetch request.
   * @returns {object} Exact acquisition response for the row's remaining supply.
   */
  fetchAnswerFor(fields) {
    const answer = super.fetchAnswerFor(fields)
    for (const topic of answer.responses) {
      if (![IDS.B, IDS.U].includes(topic.topicId)) continue
      for (const partition of topic.partitions) {
        if (!partition.acquiredRecords.length) continue
        partition.records = shareFetchV2.recordsHex
        partition.acquiredRecords = [acquired(H, H, 2)]
      }
    }
    return answer
  }

  /**
   * Requires one final from each established idle node, including a node with a held handler.
   * @returns {void}
   */
  assertIdleFinals() {
    assert.equal(this.row.expected.finals, 1, 'one final per eligible idle node')
    assert.deepEqual(
      this.finals()
        .map(({ node }) => node)
        .sort(),
      [0, 1]
    )
    assert.equal(
      new Set(
        this.observer.writes
          .filter(({ fields }) => fields.shareSessionEpoch === -1)
          .map(({ authority }) => authority.episode)
      ).size,
      1,
      'one original closing episode'
    )
  }

  /**
   * Joins with node 0 holding A0/H, node 1 finishing A1 and A2, and node 1 idle and established.
   * @param {string[]} [held] Records whose handlers wait.
   * @param {number} [calls=5] Handler entries to wait for.
   * @returns {Promise<void>}
   */
  async establish(held = [A0_H], calls = 5) {
    this.supplyOnce('A:0', 'A:1', 'A:2')
    for (const identity of held) this.hold(identity)
    await this.join()
    await until(() => this.calls.length >= calls, 'the original handlers')
    const idle = held.includes(A1_H) ? 0 : 1
    await until(
      () =>
        this.frames(78).some(({ node, fields }) => node === idle && fields.shareSessionEpoch >= 1),
      'the idle node acknowledgement round'
    )
    await flush()
  }

  /**
   * Holds the next frame matching `match`, moves the fake clock to the heartbeat tick that writes it
   * and waits until it is held.
   * @param {(call: object) => boolean} match Selects the heartbeat frame.
   * @param {number} [ms=50] Fake time to the frame.
   * @returns {Promise<object>} The held gate.
   */
  async holdBeat(match, ms = 50) {
    const gate = this.holdWire({ api: 'ShareGroupHeartbeat', match })
    if (ms) await this.pump(ms)
    await this.arrival(gate)
    await flush()
    return gate
  }

  /** @returns {Promise<void>} Reaches the row's phase before the fault. */
  async reach() {
    await this.startClient()
    const { step } = this.row
    if (this.family === 'fault' && step === 'before join') {
      this.supplyOnce('A:0')
      return
    }
    if (this.family === 'fault' || this.family === 'guard') await this.establish()
    if (this.family === 'assignment') {
      const hold = this.row.fault === 'empty assignment' ? [A1_H] : [A0_H]
      await this.establish(hold, this.row.fault === 'empty assignment' ? 3 : 5)
    }
    if (this.family === 'target') await this.establish([A1_H], 3)
  }

  /** @returns {Promise<void>} Installs the row's fault, answer or transport failure. */
  async inject() {
    await this[`inject_${this.family}`]()
  }

  /** @returns {Promise<void>} Verifies the primary outcome. */
  async verify() {
    await this[`verify_${this.family}`]()
  }

  /** @returns {Promise<void>} Proves a current replacement works. */
  async replacement() {
    await this[`replacement_${this.family}`]()
  }

  /** @returns {Promise<void>} The heartbeat fault family: inject. */
  async inject_fault() {
    if (this.row.expected.action === 'ignored') return this.injectIgnored()
    const { step } = this.row
    this.lookupsBefore = this.lookups()
    const answer = this.code === null ? null : { errorCode: this.code }
    if (step === 'before join') {
      const gate = this.armTransport({ api: 'ShareGroupHeartbeat' })
      this.runPromise = this.run()
      await this.arrival(gate)
      this.lookupsBefore = this.lookups()
      assert.deepEqual(this.calls, [], 'no acquisition before the failed join')
      assert.equal(this.frames(78, 79).length, 0)
      this.faultedFrame = await this.release_fault(gate, answer)
      return
    }
    if (step === 'assignment change') {
      this.beatShape = (fields, base) =>
        this.faulted && fields.memberEpoch > 0
          ? { ...base, assignment: { topicPartitions: [{ topicId: IDS.A, partitions: [0, 1] }] } }
          : base
    }
    if (step === 'leave') {
      const gate = this.armTransport({ api: 'ShareGroupHeartbeat', match: LEAVE })
      this.disconnecting = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.pump(300)
      await this.arrival(gate)
      await flush()
      this.faultedFrame = await this.release_fault(gate, answer)
      return
    }
    if (this.row.expected.action === 'rejoin') this.supplyOnce('A:0')
    const gate = this.armTransport({ api: 'ShareGroupHeartbeat', match: STEADY })
    await this.pump(50)
    await this.arrival(gate)
    await flush()
    this.faultedFrame = await this.release_fault(gate, answer)
  }

  /**
   * @param {object} gate The held heartbeat answer.
   * @param {object|null} answer The coded answer, or null for the row's transport failure.
   * @returns {Promise<object>} The frame the fault hit.
   */
  async release_fault(gate, answer) {
    const call = gate.call
    this.faultAuthority = this.observer.writes.findLast(({ apiKey }) => apiKey === 76).authority
    if (answer) gate.release(answer)
    else await this.fireTransport(this.transport)
    this.faulted = true
    await flush()
    return call
  }

  /** @returns {object} The first heartbeat frame's fields, which fix the run's member identity. */
  firstBeat() {
    return this.frames(76)[0].fields
  }

  /**
   * @param {object} fields A heartbeat request.
   * @param {number} epoch Expected member epoch.
   * @returns {void}
   * @throws {Error} When the frame is not a full-field frame of this run's member.
   */
  assertFullFrame(fields, epoch) {
    assert.equal(fields.memberEpoch, epoch)
    assert.equal(fields.memberId, this.firstBeat().memberId)
    assert.deepEqual(fields.subscribedTopicNames, [NAME])
    assert.equal(fields.rackId, 'rack-a')
  }

  /** @returns {Promise<void>} The heartbeat fault family: primary outcome per action. */
  async verify_fault() {
    const { action } = this.row.expected
    if (action === 'ignored') return this.verifyIgnored()
    if (action === 'left') return this.verifyLeft()
    if (action === 'fatal') return this.verifyFatal()
    return this.verifyRecovered(action === 'rejoin')
  }

  /**
   * Retry and rejoin: the member and run survive, the next frame repeats every field, a coordinator
   * code forgets and looks up the coordinator again, and a fence retires old ordinary work first.
   * @param {boolean} rejoin Whether the fault is a fence code that rejoins at epoch zero.
   * @returns {Promise<void>}
   */
  async verifyRecovered(rejoin) {
    const { step } = this.row
    const before = step === 'before join'
    const faulted = this.frames(76).length
    if (before) assert.equal(this.runState.settled, false, 'the join stays pending')
    const held = this.calls.find((call) => call.identity === A0_H)
    if (!before) assert.equal(held.isRunning(), !rejoin, 'held handler authority')
    await this.advanceUntil(() => this.frames(76).length > faulted, 'the next heartbeat')
    const next = this.frames(76)[faulted].fields
    this.assertFullFrame(next, before || rejoin ? 0 : 1)
    const recovered = [...this.observer.runs].at(-1)
    assert.equal(recovered, this.faultAuthority.run, 'recovery keeps the original run')
    if (before || rejoin)
      assert.notEqual(recovered.membership.token, this.faultAuthority.membership)
    else assert.equal(recovered.membership.token, this.faultAuthority.membership)
    const forgets = [14, 15, 16, 25, 110].includes(this.code)
    assert.equal(this.lookups(), this.lookupsBefore + (forgets ? 1 : 0), 'coordinator lookups')
    assert.equal(this.seen.crashes.length, 0)
    if (before) {
      await until(() => this.runState.settled, 'the first join')
      assert.equal(this.runState.error, null)
      assert.deepEqual(
        new Set(this.joins().map(({ fields }) => fields.memberId)),
        new Set([this.firstBeat().memberId])
      )
    }
    if (rejoin && !before) {
      this.assertIdleFinals()
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
      assert.equal(this.joins().length, 2)
    } else assert.equal(this.finals().length, 0)
    if (this.row.step === 'assignment change' && !rejoin) {
      await this.advanceUntil(
        () => this.h.consumer.assignment()[0]?.partitions.length === 2,
        'the shrunk assignment'
      )
    }
  }

  /** @returns {Promise<void>} Fatal codes: one crash with restart false and the exact cause. */
  async verifyFatal() {
    const { step, expected } = this.row
    await until(() => this.seen.crashes.length === 1, 'the crash event')
    await flush()
    assert.equal(this.seen.crashes.length, 1)
    const [crash] = this.seen.crashes
    assert.equal(crash.restart, false)
    if (expected.cause === 35) {
      assert.equal(crash.error.reason, 'refused-by-broker')
      assert.equal(crash.error.apiName, 'ShareGroupHeartbeat')
    } else assert(codesOf(crash.error).includes(expected.cause))
    assert.equal(this.restartErrors.length, 1)
    assert.equal(this.restartErrors[0], crash.error)
    if (step === 'before join') {
      await until(() => this.runState.settled, 'the rejected run')
      assert.equal(this.runState.error, crash.error)
      assert.equal(this.frames(76).length, 1)
    } else {
      this.assertIdleFinals()
      assert.equal(this.leaves().length <= 1, true)
      const held = this.calls.find((call) => call.identity === A0_H)
      assert.equal(held.isRunning(), false)
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
    }
    const frames = this.frames(76).length
    await this.pump(this.rounds * 50)
    assert.equal(this.frames(76).length, frames, 'no heartbeat after a fatal verdict')
    assert.equal(this.seen.crashes.length, 1)
  }

  /** @returns {Promise<void>} Leave: one epoch -1 attempt, no rejoin, terminal close, no crash. */
  async verifyLeft() {
    await until(() => this.disconnecting.settled, 'the disconnect')
    assert.equal(this.disconnecting.error, null)
    assert.equal(this.leaves().length, this.row.expected.finals)
    const [leave] = this.leaves()
    assert.equal(leave.fields.subscribedTopicNames, null)
    assert.equal(leave.fields.rackId, null)
    assert.deepEqual(
      this.finals()
        .map(({ node }) => node)
        .sort(),
      [0, 1]
    )
    const held = this.calls.find((call) => call.identity === A0_H)
    assert.equal(held.isRunning(), false)
    assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
    const frames = this.frames(76).length
    await this.pump(this.rounds * 50)
    assert.equal(this.frames(76).length, frames, 'no rejoin or new heartbeat')
    assert.equal(this.seen.crashes.length, 0)
    assert.equal(this.seen.stops.length, 1)
  }

  /** @returns {Promise<void>} Fault family replacement: current work or a new run control. */
  async replacement_fault() {
    if (this.row.expected.action === 'ignored') return this.replacementIgnored()
    if (this.row.expected.member === 'same') {
      if (this.row.step === 'before join') {
        await this.advanceUntil(() => this.calls.length === 2, 'the current control handlers')
        await this.advanceUntil(() => this.acceptedBoth(0), 'the current control acceptance')
        return this.releaseAll()
      }
      this.release(A0_H)
      await this.advanceUntil(() => this.acceptedBoth(0), 'the current terminal acceptance')
      return undefined
    }
    return this.newRunControl()
  }

  /**
   * @param {number} partition Partition of topic A.
   * @returns {boolean} Whether both exact records were accepted without error.
   */
  acceptedBoth(partition) {
    const accepted = this.seen.acks.filter((ack) => !ack.error && ack.partition === partition)
    return covers(accepted, H) && covers(accepted, J)
  }

  /** @returns {Promise<void>} A public run after cleanup: new member, one fresh acquisition. */
  async newRunControl() {
    const previous = this.firstBeat().memberId
    const calls = this.calls.length
    this.releaseAll()
    await flush()
    this.supplyOnce('A:0')
    await this.run()
    await until(() => this.calls.length > calls, 'the control delivery')
    assert.notEqual(this.joins().at(-1).fields.memberId, previous)
  }

  /** @returns {void} Lets every held handler return. */
  releaseAll() {
    for (const { gate } of this.heldGates) gate.resolve()
  }

  /** @returns {Promise<void>} Assignment cells: inject. */
  async inject_assignment() {
    const { fault } = this.row
    this.metadataBefore = this.cluster.requests().filter(({ api }) => api === 'Metadata').length
    this.assignedBefore = this.seen.assigned.length
    if (fault === 'unknown assignment') {
      this.h.table.set('late-topic', {
        topicId: IDS.U,
        partitions: [{ leader: 0, epoch: 1 }],
      })
      this.supplyOnce('U:0')
      this.flight = this.holdWire({
        api: 'Metadata',
        match: ({ fields }) => (fields.topics || []).some((topic) => topic.topicId === IDS.U),
      })
    }
    const gate = await this.holdBeat(STEADY)
    const { call } = gate
    if (fault === 'unknown assignment')
      gate.release(assignmentAnswer(call, [0, 1, 2], { unknown: true }))
    if (fault === 'empty assignment') gate.release(assignmentAnswer(call, null))
    if (fault === 'equal assignment') gate.release(assignmentAnswer(call, [0, 1, 2]))
    await flush()
  }

  /** @returns {string[]} Original assignment UUIDs and partitions as sorted alias keys. */
  assignedKeys() {
    const run = [...this.observer.runs].at(-1)
    assert.deepEqual(this.h.consumer.assignment(), run.assignmentSnapshot())
    return run.membership
      .snapshot()
      .assignment.flatMap(({ topicId, partitions }) =>
        partitions.map((partition) => `${ALIAS.get(topicId)}:${partition}`)
      )
      .sort()
  }

  /** @returns {Promise<void>} Assignment cells: primary outcome. */
  async verify_assignment() {
    const { fault, expected } = this.row
    if (fault === 'unknown assignment') {
      await this.arrival(this.flight)
      assert.deepEqual(this.assignedKeys(), expected.assignment, 'known partitions stay usable')
      this.flight.release()
      await until(() => this.assignedKeys().includes('U:0'), 'the resolved topic')
      assert.deepEqual(this.assignedKeys(), expected.resolved)
      this.release(A0_H)
      await this.advanceUntil(
        () => this.calls.some((call) => call.identity === `U:0@${H}`),
        'the U handler'
      )
      await flush()
      assert.equal(this.calls.filter((call) => call.identity === `U:0@${H}`).length, 1)
      const refreshes = this.cluster
        .requests()
        .filter(
          ({ api, fields }) =>
            api === 'Metadata' && (fields.topics || []).some((topic) => topic.topicId === IDS.U)
        )
      assert.equal(refreshes.length, expected.refresh)
      return
    }
    if (fault === 'empty assignment') {
      assert.deepEqual(this.h.consumer.assignment(), [])
      assert.equal(this.seen.assigned.length, this.assignedBefore + expected.changes)
      assert.equal(
        this.calls.some((call) => call.alias === 'A' && call.partition === 2),
        false,
        'revoked partition starts no handler'
      )
      this.release(A1_H)
      const released = () =>
        this.frames(78, 79).some((frame) =>
          (frame.fields.topics || []).some((topic) =>
            topic.partitions.some(
              (partition) =>
                partition.partitionIndex === 2 &&
                partition.acknowledgementBatches.some(
                  (batch) =>
                    batch.firstOffset === H &&
                    batch.lastOffset === J &&
                    batch.acknowledgeTypes.every((type) => type === 2)
                )
            )
          )
        )
      await this.advanceUntil(released, 'the release of the unstarted A2 records')
      assert.equal(
        this.calls.some((call) => call.alias === 'A' && call.partition === 2),
        false
      )
      return
    }
    await flush()
    assert.equal(this.seen.assigned.length, this.assignedBefore + expected.changes)
    assert.deepEqual(this.assignedKeys(), expected.assignment)
    assert.equal(
      this.cluster.requests().filter(({ api }) => api === 'Metadata').length,
      this.metadataBefore,
      'an equal target starts no metadata work'
    )
    const gate = await this.holdBeat(STEADY)
    gate.release({ errorCode: 0, assignment: null, memberEpoch: 1, heartbeatIntervalMs: 50 })
    await flush()
    assert.equal(this.seen.assigned.length, this.assignedBefore)
    assert.deepEqual(this.assignedKeys(), expected.assignment)
  }

  /** @returns {Promise<void>} Assignment cells: a current replacement acquisition. */
  async replacement_assignment() {
    if (this.row.fault !== 'equal assignment') {
      this.releaseAll()
      await flush()
      return
    }
    this.release(A0_H)
    await until(
      () => this.seen.acks.some((ack) => !ack.error && ack.partition === 0 && covers([ack], J)),
      'the current terminal acceptance'
    )
  }

  /**
   * Records calls and effects of the original Cluster helpers, cache and membership publication.
   * @returns {void} Every observer delegates with its original receiver and arguments.
   */
  observeMetadata() {
    this.ownerRun = [...this.observer.runs].at(-1)
    this.member = this.ownerRun.membership
    this.realCluster = this.ownerRun.options.cluster
    jest.spyOn(Date, 'now').mockReturnValue(1000)
    this.metadataCalls = []
    this.metadataEffects = []
    for (const method of ['refreshMetadataForTopicIds', 'fetchMetadataByTopicIds']) {
      this.observer.watch(this.realCluster, method, (receiver, args, next) => {
        const captured = {
          method,
          receiver,
          args,
          token: this.member.token,
          target: this.member.targetRevision,
          at: performance.now(),
          settled: false,
        }
        this.metadataCalls.push(captured)
        return this.observe(
          Promise.resolve(next()).then((result) => {
            captured.settled = true
            return result
          }),
          'metadata helper'
        )
      })
    }
    for (const method of ['update', 'updateTopicsById']) {
      this.observer.watch(this.realCluster.metadataCache, method, (receiver, args, next) => {
        this.metadataEffects.push({ method, receiver, args })
        return next()
      })
    }
    this.observer.watch(this.member, 'publishAssignment', (receiver, args, next) => {
      this.metadataEffects.push({ method: 'publishAssignment', receiver, args })
      return next()
    })
  }

  /**
   * Takes immutable values alongside original issuance identities for a metadata/heartbeat hold.
   * @returns {object} Cache, target, unresolved, interval and public-event observations.
   */
  metadataSnapshot() {
    const cache = this.realCluster.metadataCache
    return {
      run: this.ownerRun,
      token: this.member.token,
      targetRevision: this.member.targetRevision,
      target: structuredClone(this.member.target),
      unresolved: this.member.unresolved.slice(),
      assignment: structuredClone(this.member.assignment),
      interval: this.member.interval,
      clock: Date.now(),
      targets: [...this.realCluster.targetTopics].sort(),
      ids: [...cache.topicIds].sort(),
      topics: [...cache.topics].map(([name, entry]) => [
        name,
        entry.topicId,
        [...entry.partitions].map(([id, value]) => [id, value.leader, value.leaderEpoch]),
      ]),
      assigned: this.seen.assigned.length,
      beats: this.seen.beats.length,
      calls: this.calls.map(({ identity }) => identity),
      effects: this.metadataEffects.length,
      helpers: this.metadataCalls.length,
      requests: this.cluster.requests().filter(({ api }) => api === 'Metadata').length,
    }
  }

  /**
   * Builds a complete literal metadata response at the actual wire seam.
   * @param {object[]} topics Topic identities and error codes to serve.
   * @returns {object} Metadata response with no inherited error or throttle fields.
   */
  metadataReply(topics) {
    return {
      throttleTime: 0,
      brokers: this.cluster.nodes.map(({ id, host, port }) => ({
        nodeId: id,
        host,
        port,
        rack: null,
      })),
      controllerId: 0,
      topics: topics.map(({ topicId, name, errorCode = 0 }) => ({
        topicId,
        name,
        errorCode,
        isInternal: false,
        topicAuthorizedOperations: -2147483648,
        partitions: errorCode
          ? []
          : [
              {
                errorCode: 0,
                partitionIndex: 0,
                leaderId: 0,
                leaderEpoch: 1,
                replicas: [0],
                isr: [0],
                offlineReplicas: [],
              },
            ],
      })),
    }
  }

  /**
   * Reaches a distinct bulk, per-ID, by-name or post-helper publication continuation.
   * Heartbeat identity and retry rows have separate schedules below.
   * @returns {Promise<void>}
   */
  async inject_target() {
    this.observeMetadata()
    this.originalTarget = this.metadataSnapshot()
    const { fault } = this.row
    if (fault === 'equal epoch old answer') {
      this.flight = await this.holdBeat(STEADY)
      this.originalBeat = this.flight.call
      await this.supersedeByStop()
      return
    }
    if (fault === 'lower epoch answer') return this.lowerEpochAnswer()
    if (fault === 'known ids while unknown retries' || fault === 'same target preserves backoff')
      return this.repeatUnknown(fault === 'same target preserves backoff')

    const perId = fault === 'held per-id metadata'
    const bulk = perId || fault === 'held bulk metadata'
    if (fault === 'held by-name metadata') {
      const original = Broker.prototype.negotiatedVersion
      jest
        .spyOn(Broker.prototype, 'negotiatedVersion')
        .mockImplementation(function (key, definition, caps) {
          return original.call(this, key, definition, key === 3 ? { ...caps, 3: 11 } : caps)
        })
    }
    if (perId) {
      this.h.schedule.once(
        { api: 'Metadata', match: ({ fields }) => fields.topics?.length === 2 },
        this.metadataReply([
          { topicId: IDS.U, name: 'late-topic' },
          { topicId: IDS.B, name: NAME, errorCode: 100 },
        ])
      )
    }
    if (fault !== 'held unresolved publication') {
      this.metadataContinuation = deferred()
      this.heldGates.push({ identity: 'original-metadata-result', gate: this.metadataContinuation })
      this.observer.watch(Broker.prototype, 'metadata', async (receiver, [topics = []], next) => {
        const result = await next()
        const selected =
          fault === 'held by-name metadata'
            ? topics.length === 0
            : topics.length === (perId ? 1 : 2) &&
              topics.some(({ topicId }) => topicId === (perId ? IDS.B : IDS.U))
        if (selected && !this.metadataReached) {
          this.metadataReached = { receiver, topics, result }
          await this.metadataContinuation.promise
        }
        return result
      })
    }
    this.flight = this.holdWire({
      api: 'Metadata',
      match: ({ fields }) => {
        if (fault === 'held by-name metadata') return fields.topics === null
        return (
          fields.topics?.length === (perId ? 1 : bulk ? 2 : 1) &&
          fields.topics.some(({ topicId }) => topicId === (perId ? IDS.B : IDS.U))
        )
      },
    })
    if (fault === 'held unresolved publication') {
      this.publication = deferred()
      this.heldGates.push({ identity: 'unresolved-publication', gate: this.publication })
      this.observer.watch(
        this.realCluster,
        'refreshMetadataForTopicIds',
        async (receiver, args, next) => {
          const result = await next()
          if (!this.publicationReached && args[0].includes(IDS.U)) {
            this.publicationReached = true
            await this.publication.promise
          }
          return result
        }
      )
    }
    const beat = await this.holdBeat(STEADY)
    beat.release(
      assignmentAnswer(beat.call, [0, 1, 2], {
        unknown: true,
        extra: bulk ? [{ topicId: IDS.B, partitions: [0] }] : [],
      })
    )
    await this.arrival(this.flight)
    this.issuedMetadata = this.metadataCalls.find(
      ({ method }) => method === 'refreshMetadataForTopicIds'
    )
    assert.equal(this.issuedMetadata.receiver, this.realCluster)
    assert.equal(this.issuedMetadata.token, this.member.token)
    assert.equal(this.issuedMetadata.target, this.member.targetRevision)
    assert.equal(this.issuedMetadata.args[1].isCurrent(), true)
    assert.deepEqual(
      this.issuedMetadata.args[0].slice().sort(),
      bulk ? [IDS.B, IDS.U].sort() : [IDS.U]
    )
    if (perId) {
      assert.deepEqual(
        this.flight.call.fields.topics.map(({ topicId }) => topicId),
        [IDS.B],
        'topic-level bulk failure reached the first original per-ID request'
      )
    }
    if (fault === 'held unresolved publication') {
      this.flight.release(this.metadataReply([{ topicId: IDS.U, name: 'late-topic' }]))
      await until(() => this.publicationReached, 'the original unresolved publication continuation')
      assert.equal(this.realCluster.topicName(IDS.U), 'late-topic')
      assert.deepEqual(this.member.unresolved, [IDS.U])
      await this.supersedeByRecreate()
    } else {
      this.flight.release(
        this.metadataReply([
          { topicId: IDS.U, name: 'late-topic' },
          { topicId: IDS.B, name: NAME },
        ])
      )
      await until(() => this.metadataReached !== undefined, 'the original decoded metadata result')
      assert(this.metadataReached.receiver instanceof Broker, 'original selected metadata broker')
      if (fault === 'held bulk metadata') await this.supersedeByStop(400)
      else await this.supersedeByFence(fault === 'held by-name metadata')
    }
    assert.equal(
      this.issuedMetadata.args[1].isCurrent(),
      false,
      'original nested predicate retired'
    )
  }

  /**
   * Retires the original membership through public disconnect, including a real leave when budgeted.
   * @param {number} [timeout=0] Public close budget.
   * @returns {Promise<void>}
   */
  async supersedeByStop(timeout = 0) {
    this.supersession = 'stop'
    this.disconnecting = track(this.h.consumer.disconnect({ timeout }))
    if (timeout) await this.pump(300)
    await until(() => this.disconnecting.settled, 'the disconnect')
    assert.equal(this.disconnecting.error, null)
    if (timeout)
      assert.equal(this.leaves().length, 1, 'bulk metadata is held across the real leave')
  }

  /**
   * Fences the original membership through an actual heartbeat response.
   * @param {boolean} [rejoined=false] Wait for the replacement join while the helper remains held.
   * @returns {Promise<void>}
   */
  async supersedeByFence(rejoined = false) {
    this.supersession = rejoined ? 'rejoined' : 'fence'
    const gate = await this.holdBeat(STEADY)
    gate.release({ errorCode: 110 })
    await flush()
    if (rejoined) await this.advanceUntil(() => this.joins().length === 2, 'the rejoin')
    assert.notEqual(this.member.token, this.originalTarget.token)
  }

  /**
   * Changes the target to UUID B at the same name and epoch while old publication is suspended.
   * The original A cache is still present, so the B resolution must use its own guarded helper.
   * @returns {Promise<void>}
   */
  async supersedeByRecreate() {
    this.supersession = 'replacement'
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 1 }] })
    this.supplyOnce('B:0')
    this.beatShape = (fields) => assignmentAnswer({ fields }, [0], { topicId: IDS.B })
    const gate = await this.holdBeat(STEADY)
    gate.release(assignmentAnswer(gate.call, [0], { topicId: IDS.B }))
    await this.advanceUntil(
      () => this.calls.some(({ alias }) => alias === 'B'),
      'current B delivery'
    )
    assert.notEqual(this.member.targetRevision, this.originalTarget.targetRevision)
    assert.equal(this.member.memberEpoch, 1, 'replacement repeats the numeric epoch')
    assert.equal(this.realCluster.topicId(NAME), IDS.B)
  }

  /**
   * A lower-epoch heartbeat cannot change interval, assignment, target, cache or public events.
   * @returns {Promise<void>}
   */
  async lowerEpochAnswer() {
    const gate = await this.holdBeat(STEADY)
    const before = this.metadataSnapshot()
    gate.release({ ...assignmentAnswer(gate.call, [0], { epoch: 0 }), heartbeatIntervalMs: 999 })
    await flush()
    assert.deepEqual(this.metadataSnapshot(), before, 'lower-epoch answer has no effects')
    this.lowerChecked = true
  }

  /**
   * Proves completed unresolved misses arm metadata backoff independently of healthy heartbeats.
   * Repeated identical targets preserve the original revision and due time; A remains usable.
   * @param {boolean} same Repeat the identical assignment at every heartbeat.
   * @returns {Promise<void>}
   */
  async repeatUnknown(same) {
    const originalMetadata = this.cluster.answerers.get('Metadata')
    this.cluster.answer('Metadata', (fields) => {
      assert(
        fields.topics?.every(({ topicId }) => topicId === IDS.U),
        'only unresolved U is retried'
      )
      return this.metadataReply([{ topicId: IDS.U, name: 'late-topic', errorCode: 100 }])
    })
    const beat = await this.holdBeat(STEADY)
    beat.release(assignmentAnswer(beat.call, [0, 1, 2], { unknown: true }))
    await until(
      () => this.member.resolution === null && this.member.metadataBackoff.attempts === 1,
      'the completed unresolved miss'
    )
    const revision = this.member.targetRevision
    const token = this.member.token
    const due = this.member.metadataBackoff.until
    const first = this.metadataCalls.filter(({ method }) => method === 'refreshMetadataForTopicIds')
    assert.equal(first.length, 1)
    assert.equal(due, first[0].at + 250, 'metadata retry deadline')
    const hold = this.holdWire({
      api: 'Metadata',
      match: ({ fields }) => fields.topics?.some(({ topicId }) => topicId === IDS.U),
    })
    this.release(A1_H)
    await until(
      () => this.acceptedBoth(1) && this.acceptedBoth(2),
      'known A deliveries and results'
    )
    for (let n = 0; n < 4; n++) {
      const next = await this.holdBeat(STEADY)
      const partitions = same || n % 2 === 0 ? [0, 1, 2] : [2, 0, 1, 1]
      next.release(assignmentAnswer(next.call, partitions, { unknown: true }))
      await flush()
      assert.equal(this.member.token, token)
      assert.equal(
        this.member.targetRevision,
        revision,
        'semantic target retains original identity'
      )
      assert.equal(
        this.member.metadataBackoff.until,
        due,
        'heartbeat did not restart metadata backoff'
      )
      assert.equal(
        this.metadataCalls.filter(({ method }) => method === 'refreshMetadataForTopicIds').length,
        1
      )
      assert.equal(this.member.backoff.attempts, 0, 'healthy heartbeat has independent backoff')
    }
    await this.pump(Math.max(0, due - performance.now() - 1))
    assert.equal(hold.call, null, 'no metadata retry before due time')
    await this.advanceUntil(() => hold.call !== null, 'the due metadata retry')
    assert.equal(
      this.metadataCalls.filter(({ method }) => method === 'refreshMetadataForTopicIds').length,
      2
    )
    assert.equal(
      this.metadataCalls.filter(({ method }) => method === 'refreshMetadataForTopicIds')[1].at,
      due,
      'the first eligible retry starts at the retained deadline'
    )
    assert.equal(this.member.metadataBackoff.attempts, 1, 'held retry is still single flight')
    assert.equal(
      this.calls.some(({ alias }) => alias === 'U'),
      false
    )
    hold.release(this.metadataReply([{ topicId: IDS.U, name: 'late-topic', errorCode: 100 }]))
    await until(
      () => this.member.resolution === null && this.member.metadataBackoff.attempts === 2,
      'the second completed miss'
    )
    this.cluster.answer('Metadata', originalMetadata)
    this.retryChecked = true
  }

  /**
   * Releases only the old continuation and compares every observed nested effect and follow-up.
   * Equal-epoch heartbeat release is deferred until the replacement run exists.
   * @returns {Promise<void>}
   */
  async verify_target() {
    if (this.row.fault === 'equal epoch old answer' || this.lowerChecked || this.retryChecked)
      return
    const before = this.metadataSnapshot()
    if (this.publication) this.publication.resolve()
    else this.metadataContinuation.resolve()
    await until(() => this.issuedMetadata.settled, 'the original metadata helper completion')
    await flush()
    assert.deepEqual(
      this.metadataSnapshot(),
      before,
      'old metadata has zero cache/publication/follow-up effects'
    )
    assert.equal(this.seen.crashes.length, 0)
    const old = this.calls.find(({ identity }) => identity === this.row.expected.old[0])
    assert(old, 'the original A invocation reached the hold')
    if (this.supersession !== 'replacement') {
      assert.equal(old.isRunning(), false)
      assert.throws(() => old.acknowledge('accept'), { message: STALE_CALL })
    }
  }

  /**
   * Completes the old A handlers before proving a single B0/H handler and accepted terminal result.
   * The old heartbeat is released only after the new run repeats its epoch and still owns B.
   * @returns {Promise<void>}
   */
  async replacement_target() {
    if (!this.calls.some(({ alias }) => alias === 'B')) {
      this.releaseAll()
      await flush()
      if (this.lowerChecked || this.retryChecked) {
        await this.advanceUntil(
          () => [0, 1, 2].every((partition) => this.acceptedBoth(partition)),
          'the complete current A control'
        )
      }
      await this.supersedeByStop()
      this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 1 }] })
      this.supplyOnce('B:0')
      this.beatShape = (fields) => assignmentAnswer({ fields }, [0], { topicId: IDS.B })
      await this.run()
      await this.advanceUntil(
        () => this.calls.some(({ alias }) => alias === 'B'),
        'the B control handler'
      )
    }
    this.releaseAll()
    await this.advanceUntil(
      () =>
        this.observer.published.some(
          ({ cohort, event }) =>
            cohort.topicId === IDS.B &&
            event.firstOffset === H &&
            event.lastOffset === H &&
            !event.error
        ),
      'the exact B control acceptance'
    )
    assert.equal(
      this.calls.filter(({ identity }) => identity === this.row.expected.current[0]).length,
      1
    )
    assert.equal(
      this.calls.some(({ alias }) => alias === 'U'),
      false,
      'no unresolved handler escaped'
    )
    const replacement = [...this.observer.runs].at(-1)
    const old = this.calls.find(({ identity }) => identity === this.row.expected.old[0])
    const currentCall = this.calls.find(({ identity }) => identity === this.row.expected.current[0])
    assert(old, 'the original target handler was captured')
    assert.notEqual(currentCall.token, old.token, 'replacement has its own original acquisition')
    assert.notEqual(currentCall.providerCall, old.providerCall)
    assert.equal(Date.now(), this.originalTarget.clock, 'replacement repeats the wall clock')
    assert.notEqual(replacement.membership.targetRevision, this.originalTarget.targetRevision)
    if (this.row.fault === 'equal epoch old answer') {
      const before = this.metadataSnapshot()
      const current = replacement.membership.snapshot()
      assert.equal(current.memberEpoch, this.originalBeat.fields.memberEpoch)
      assert.notEqual(replacement, this.ownerRun)
      assert.notEqual(current.token, this.originalTarget.token)
      this.flight.release(assignmentAnswer(this.originalBeat, [0, 1, 2]))
      await flush()
      assert.deepEqual(
        this.metadataSnapshot(),
        before,
        'equal-epoch old heartbeat has zero effects'
      )
      assert.deepEqual(
        replacement.membership.snapshot(),
        current,
        'replacement membership survives old answer'
      )
      assert.equal(this.realCluster.topicId(NAME), IDS.B)
    }
  }

  /** @returns {Promise<void>} Write-boundary cells: hold the next heartbeat at its stage. */
  async inject_guard() {
    this.steadyBefore = this.steadyFrames()
    this.boundary = this.row.fault.replace('held heartbeat ', '')
    this.pending = this.installBoundary(this.boundary)
    await this.pump(50)
    await until(() => this.pending.reached(), `the ${this.boundary} boundary`)
    await flush()
    if (this.row.step === 'fence') {
      this.supersession = 'fence'
      this.h.cluster.answer('ShareFetch', (fields) =>
        fields.shareSessionEpoch >= 1 && !this.fenced
          ? ((this.fenced = true), topError(25))
          : this.fetchAnswerFor(fields)
      )
      await this.pump(50)
      await until(() => this.fenced, 'the fence answer')
      await flush()
    } else {
      this.supersession = 'stop'
      this.disconnecting = track(this.h.consumer.disconnect({ timeout: 0 }))
      await until(() => this.disconnecting.settled, 'the zero-deadline disconnect')
    }
  }

  /** @returns {number} Steady heartbeat frames written so far. */
  steadyFrames() {
    return this.frames(76).filter(({ fields }) => fields.memberEpoch > 0).length
  }

  /**
   * @param {'connect'|'encode'|'queue'|'response'} boundary The write stage to hold.
   * @returns {{reached: () => boolean, release: () => void}} The hold.
   */
  installBoundary(boundary) {
    let reached = false
    if (boundary === 'response') {
      const wire = this.holdWire({ api: 'ShareGroupHeartbeat', match: STEADY })
      wire.arrived().then(() => {
        reached = true
      })
      return { reached: () => reached, release: () => wire.settle && wire.release() }
    }
    const gate = deferred()
    let taken = false
    const take = () => {
      if (taken) return false
      taken = true
      reached = true
      return true
    }
    if (boundary === 'connect') {
      const original = ConnectionPool.prototype.getConnectionByRequest
      jest.spyOn(ConnectionPool.prototype, 'getConnectionByRequest').mockImplementation(function (
        protocolRequest,
        ...rest
      ) {
        const pending = original.call(this, protocolRequest, ...rest)
        if (protocolRequest?.request?.apiKey !== 76 || taken) return pending
        return pending.then(async (connection) => {
          take()
          await gate.promise
          return connection
        })
      })
    } else if (boundary === 'encode') {
      const original = requests.ShareGroupHeartbeat.protocol
      jest.spyOn(requests.ShareGroupHeartbeat, 'protocol').mockImplementation((selection) => {
        const factory = original(selection)
        return (options) => {
          const built = factory(options)
          if (!(options?.memberEpoch > 0) || taken) return built
          const { encode } = built.request
          return {
            ...built,
            request: {
              ...built.request,
              encode: async () => {
                const encoded = await encode()
                take()
                await gate.promise
                return encoded
              },
            },
          }
        }
      })
    } else {
      const original = RequestQueue.prototype.sendSocketRequest
      const scenario = this
      jest
        .spyOn(RequestQueue.prototype, 'sendSocketRequest')
        .mockImplementation(function (request) {
          if (request.entry.apiKey !== 76 || !take()) return original.call(this, request)
          scenario.observe(
            gate.promise.then(() => original.call(this, request)),
            'held transmission'
          )
          return undefined
        })
    }
    this.heldGates.push({ identity: `boundary:${boundary}`, gate })
    return { reached: () => reached, release: () => gate.resolve() }
  }

  /** @returns {Promise<void>} Write-boundary cells: old frames, closing frames, late effects. */
  async verify_guard() {
    const { expected } = this.row
    this.pending.release()
    await flush()
    assert.equal(
      this.steadyFrames() - this.steadyBefore,
      expected.oldWrites,
      'old frames written for the held boundary'
    )
    assert.equal(this.finals().length, expected.closingWrites, 'closing frames')
    assert.equal(this.seen.crashes.length, 0)
  }

  /** @returns {Promise<void>} Write-boundary cells: the replacement membership delivers B0/H. */
  async replacement_guard() {
    const identity = this.row.expected.replacement[0]
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    if (this.supersession === 'fence')
      await this.advanceUntil(() => this.joins().length >= 2, 'the rejoin')
    else await this.run()
    await this.advanceUntil(
      () => this.calls.some((call) => call.identity === identity),
      'the replacement delivery'
    )
    await flush()
    assert.equal(this.calls.filter((call) => call.identity === identity).length, 1)
    this.releaseAll()
  }

  /**
   * A superseded or already-left membership answered with code 69: the late answer is ignored.
   * @returns {Promise<void>}
   */
  async injectIgnored() {
    const gate = await this.holdBeat(STEADY)
    this.lateGate = gate
    if (this.row.fault.endsWith('unsubscribed')) {
      this.supersession = 'stop'
      this.disconnecting = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.pump(300)
      await until(() => this.disconnecting.settled, 'the leave')
    } else {
      this.supersession = 'fence'
      this.supplyOnce('A:0')
      this.h.cluster.answer('ShareFetch', (fields) =>
        fields.shareSessionEpoch >= 1 && !this.fenced
          ? ((this.fenced = true), topError(25))
          : this.fetchAnswerFor(fields)
      )
      await this.pump(50)
      await until(() => this.fenced, 'the fence answer')
      await flush()
    }
    this.framesBefore = this.frames(76).length
    gate.release({ errorCode: 69 })
    await flush()
  }

  /** @returns {Promise<void>} The late 69 changes nothing: no crash, no restart, no extra frame. */
  async verifyIgnored() {
    assert.equal(this.seen.crashes.length, 0)
    assert.equal(this.restartErrors.length, 0)
    if (this.supersession === 'stop') {
      assert.equal(this.frames(76).length, this.framesBefore, 'no rejoin after the leave')
      assert.equal(this.leaves().length, 1)
      assert.equal(this.seen.stops.length, 1)
    } else {
      await this.advanceUntil(() => this.joins().length === 2, 'the replacement membership')
      assert.equal(this.joins()[1].fields.memberId, this.firstBeat().memberId)
      assert.equal(this.seen.crashes.length, 0)
    }
  }

  /** @returns {Promise<void>} After an ignored 69 a current membership or run still works. */
  async replacementIgnored() {
    if (this.supersession === 'stop') return this.newRunControl()
    const original = this.calls.find((call) => call.identity === A0_H)
    await this.advanceUntil(
      () => this.calls.some((call) => call.identity === A0_H && call.token !== original.token),
      'the replacement delivery'
    )
    this.releaseAll()
    return undefined
  }

  /**
   * Completes every literal outcome and, for write guards, proves a current captured final.
   * @returns {Promise<void>} Replays the complete transcript before strict baseline cleanup.
   */
  async assertInvariants() {
    this.releaseAll()
    await this.advanceUntil(
      () => [...this.observer.ledgers.keys()].every((ledger) => ledger.size === 0),
      'all literal terminal outcomes'
    )
    if (this.family === 'guard') {
      const before = this.finals().length
      const closing = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.advanceUntil(() => closing.settled, 'the current captured final control')
      assert.equal(closing.error, null)
      assert.deepEqual(
        this.finals()
          .slice(before)
          .map(({ node }) => node),
        [0]
      )
    } else await this.h.consumer.disconnect({ timeout: 0 })
    await flush()
    this.replay()
    if (this.auditFailures.length) throw this.auditFailures[0]
    assert.deepEqual(this.unhandled, [])
  }
}

describe('share membership faults', () => {
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each(rows)('$fault at $step (row $#)', async (row) => {
    const scenario = new MembershipFaultCase(row)
    const failures = []
    try {
      await scenario.reach()
      await scenario.inject()
      await scenario.verify()
      await scenario.replacement()
      await scenario.assertInvariants()
    } catch (error) {
      failures.push(error)
    }
    for (const finish of [() => scenario.dispose(), () => scenario.assertBaseline()]) {
      try {
        await finish()
      } catch (error) {
        failures.push(error)
      }
    }
    try {
      assert(scenario.cluster, 'scripted cluster was not created')
      scenario.cluster.assertClean()
    } catch (error) {
      failures.push(error)
    }
    if (failures.length) throw new AggregateError(failures, 'Share fault case failed')
  })
})

describe('membership fault observations', () => {
  test('retains every literal scenario once', () => {
    expect(rows).toHaveLength(81)
    expect(
      new Set(rows.map((row) => `${row.fault} / ${row.step} / ${row.expected.enforce ?? ''}`)).size
    ).toBe(81)
    expect(rows.every((row) => row.expected.rounds >= 8)).toBe(true)
  })

  test('rejects concurrent handlers of the same exact record', () => {
    const audit = new FaultAudit()
    const call = Symbol('call')
    audit.handler('A:0@9007199254740993', call, true)
    expect(() => audit.handler('A:0@9007199254740993', Symbol('replacement'), true)).toThrow(
      'concurrent'
    )
    audit.handler('A:0@9007199254740993', call, false)
    audit.handler('A:0@9007199254740993', Symbol('next'), true)
  })
  test('does not confuse adjacent offsets above the safe integer limit', () => {
    const audit = new FaultAudit()
    const first = Symbol('first')
    const second = Symbol('second')
    audit.handler('A:0@9007199254740993', first, true)
    audit.handler('A:0@9007199254740994', second, true)
    expect(audit.handlers.size).toBe(2)
    expect(() => audit.range(Number('9007199254740993'), '9007199254740993')).toThrow()
  })
  test('refuses an ordinary frame after membership retirement', () => {
    const audit = new FaultAudit()
    expect(() => audit.frame({ apiKey: 76, fields: {} }, { allowed: false })).toThrow(
      'unauthorized'
    )
    expect(() => audit.frame({ apiKey: 76, fields: {} }, { allowed: true })).not.toThrow()
  })
  test('rejects a completion from another invocation', () => {
    const audit = new FaultAudit()
    audit.handler('A:0@9007199254740993', Symbol('first'), true)
    expect(() => audit.handler('A:0@9007199254740993', Symbol('second'), false)).toThrow()
  })
})
