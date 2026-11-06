const assert = require('node:assert/strict')
const { deferred, flush, topError, until } = require('../../../../testHelpers/shareHarness')
const ConnectionPool = require('../../../network/connectionPool')
const RequestQueue = require('../../../network/requestQueue')
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

/** Literal fault, phase and outcome rows; the runtime driver must not mutate this oracle. */
const rows = [
  {
    fault: 'heartbeat 14',
    step: 'before join',
    expected: {
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

/** Heartbeat answer that assigns the partitions of topic A and, optionally, of topic U. */
const assignmentAnswer = (call, partitions, { unknown = false, epoch = 1 } = {}) => ({
  errorCode: 0,
  errorMessage: null,
  memberId: call.fields.memberId,
  memberEpoch: epoch,
  heartbeatIntervalMs: 50,
  assignment: {
    topicPartitions: [
      ...(partitions ? [{ topicId: IDS.A, partitions }] : []),
      ...(unknown ? [{ topicId: IDS.U, partitions: [0] }] : []),
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
      enforce: enforce ?? true,
    })
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
    if (this.family === 'fault' && step === 'before join') return
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
      this.supplyOnce('A:0')
      const gate = this.armTransport({ api: 'ShareGroupHeartbeat', match: LEAVE })
      this.disconnecting = track(this.h.consumer.disconnect({ timeout: 400 }))
      await this.pump(300)
      await this.arrival(gate)
      await flush()
      this.faultedFrame = await this.release_fault(gate, answer)
      return
    }
    this.supplyOnce('A:0')
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
      assert.equal(this.finals().length, this.row.expected.finals)
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
      assert.equal(this.finals().length, expected.finals)
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
      this.finals().map(({ node }) => node),
      [1]
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
        await until(() => this.calls.length >= 1, 'the first handler')
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

  /** @returns {Promise<void>} @returns {string[]} The public assignment as sorted alias keys. */
  assignedKeys() {
    return this.h.consumer
      .assignment()
      .flatMap(({ topic, partitions }) =>
        partitions.map((partition) => `${ALIAS.get(this.h.table.get(topic).topicId)}:${partition}`)
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
      await until(() => this.calls.some((call) => call.identity === `U:0@${H}`), 'the U handler')
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
   * Metadata-flight cells: an old flight of an unresolved topic is held while the membership or
   * target is replaced, then released late.
   * @returns {Promise<void>}
   */
  async inject_target() {
    const { fault } = this.row
    this.h.table.set('late-topic', { topicId: IDS.U, partitions: [{ leader: 0, epoch: 1 }] })
    this.flight = this.holdWire({
      api: 'Metadata',
      match: ({ fields }) => (fields.topics || []).some((topic) => topic.topicId === IDS.U),
    })
    const gate = await this.holdBeat(STEADY)
    gate.release(assignmentAnswer(gate.call, [0, 1, 2], { unknown: true }))
    await this.arrival(this.flight)
    await flush()
    this.assignedAtFlight = this.seen.assigned.length
    this.callsAtFlight = this.calls.length
    this.assignedKeysAtFlight = this.assignedKeys()
    const supersede = {
      'held bulk metadata': () => this.supersedeByStop(),
      'held per-id metadata': () => this.supersedeByFence(),
      'held by-name metadata': () => this.supersedeByFence(true),
      'held unresolved publication': () => this.supersedeByRecreate(),
      'equal epoch old answer': () => this.supersedeByStop(),
      'lower epoch answer': () => this.lowerEpochAnswer(),
      'known ids while unknown retries': () => this.repeatUnknown(false),
      'same target preserves backoff': () => this.repeatUnknown(true),
    }[fault]
    await supersede()
  }

  /** @returns {Promise<void>} Retires the membership by a zero-deadline disconnect. */
  async supersedeByStop() {
    this.supersession = 'stop'
    this.disconnecting = track(this.h.consumer.disconnect({ timeout: 0 }))
    await until(() => this.disconnecting.settled, 'the disconnect')
  }

  /**
   * @param {boolean} [rejoined] Whether to wait for the rejoin before the old flight is released.
   * @returns {Promise<void>} Fences the membership with code 110.
   */
  async supersedeByFence(rejoined = false) {
    this.supersession = rejoined ? 'rejoined' : 'fence'
    const gate = await this.holdBeat(STEADY)
    gate.release({ errorCode: 110 })
    await flush()
    if (rejoined) await this.advanceUntil(() => this.joins().length === 2, 'the rejoin')
  }

  /** @returns {Promise<void>} Recreates the topic under the same name while the flight is held. */
  async supersedeByRecreate() {
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    const gate = await this.holdBeat(STEADY)
    gate.release(assignmentAnswer(gate.call, null, { epoch: 2 }))
    await flush()
  }

  /** @returns {Promise<void>} A lower-epoch answer changes nothing before the flight is released. */
  async lowerEpochAnswer() {
    const gate = await this.holdBeat(STEADY)
    gate.release(assignmentAnswer(gate.call, [0], { epoch: 0 }))
    await flush()
    assert.deepEqual(this.assignedKeys(), this.assignedKeysAtFlight)
    assert.equal(this.seen.assigned.length, this.assignedAtFlight)
  }

  /**
   * @param {boolean} same Whether every repeated answer carries the identical target.
   * @returns {Promise<void>} Repeats an unresolved target; only one metadata flight may exist while
   *   the first is pending, and the known partitions keep delivering.
   */
  async repeatUnknown(same) {
    const flights = () =>
      this.cluster
        .requests()
        .filter(
          ({ api, fields }) =>
            api === 'Metadata' && (fields.topics || []).some((topic) => topic.topicId === IDS.U)
        ).length
    const before = flights()
    for (const known of same
      ? [
          [0, 1, 2],
          [0, 1, 2],
          [0, 1, 2],
        ]
      : [
          [0, 1, 2],
          [0, 1],
          [0, 1, 2],
        ]) {
      const gate = await this.holdBeat(STEADY)
      gate.release(assignmentAnswer(gate.call, known, { unknown: true }))
      await flush()
    }
    assert.equal(flights(), before, 'one metadata flight while the first is pending')
    assert(this.calls.every((call) => call.alias !== 'U'))
    await this.supersedeByRecreate()
  }

  /** @returns {Promise<void>} Metadata-flight cells: the late old flight has no effect. */
  async verify_target() {
    const { expected } = this.row
    const assigned = this.seen.assigned.length
    const callsBefore = this.calls.length
    if (this.flight.settle) this.flight.release()
    await flush()
    assert.equal(
      this.calls.slice(callsBefore).filter((call) => call.alias === 'U').length,
      expected.oldEffects,
      'the late flight starts no handler'
    )
    if (this.supersession) {
      assert.equal(this.seen.assigned.length, assigned, 'the late flight publishes nothing')
    }
    assert.equal(this.seen.crashes.length, 0)
    const old = this.calls.find((call) => call.identity === expected.old[0])
    assert(old, 'the old identity was delivered')
    if (this.supersession) assert.throws(() => old.acknowledge('accept'), { message: STALE_CALL })
  }

  /** @returns {Promise<void>} Metadata-flight cells: B0/H arrives under a distinct target. */
  async replacement_target() {
    const { expected } = this.row
    const identity = expected.current[0]
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    if (this.supersession === 'stop') await this.run()
    else if (this.supersession !== 'fence') {
      const gate = await this.holdBeat(STEADY)
      gate.release(assignmentAnswer(gate.call, null, { epoch: 2 }))
    }
    const delivered = () => this.calls.some((call) => call.identity === identity)
    await this.advanceUntil(delivered, 'the replacement delivery')
    await flush()
    assert.equal(this.calls.filter((call) => call.identity === identity).length, 1)
    assert.equal(expected.target, 'distinct')
    assert.notEqual(this.h.table.get(NAME).topicId, IDS.A)
    this.releaseAll()
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
        if (protocolRequest?.request?.apiKey !== 76 || !take())
          return original.call(this, protocolRequest, ...rest)
        return gate.promise.then(() => original.call(this, protocolRequest, ...rest))
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
                take()
                await gate.promise
                return encode()
              },
            },
          }
        }
      })
    } else {
      const original = RequestQueue.prototype.push
      jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (pushed) {
        if (pushed.entry.apiKey !== 76 || !take()) return original.call(this, pushed)
        gate.promise.then(() => {
          try {
            original.call(this, pushed)
          } catch (error) {
            pushed.entry.reject(error)
          }
        })
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
    this.supplyOnce('A:0')
    const calls = this.calls.filter((call) => call.identity === A0_H).length
    await this.advanceUntil(
      () => this.calls.filter((call) => call.identity === A0_H).length > calls,
      'the replacement delivery'
    )
    this.releaseAll()
    return undefined
  }

  /** @returns {Promise<void>} Replays frames, handlers and results through the audit. */
  async assertInvariants() {
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
      scenario.cluster?.assertClean()
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
