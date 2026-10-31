module.exports = {
  verbose: !!process.env.VERBOSE,
  moduleNameMapper: {
    // Anchored: an unanchored key matches any require path that merely contains the name, so a
    // module under testHelpers/ would resolve to the index instead of itself.
    '^testHelpers$': '<rootDir>/testHelpers/index.js',
  },
  setupFilesAfterEnv: ['<rootDir>/testHelpers/setup.js'],
  // Only these trees hold specs; keeps stray local directories out of test discovery.
  roots: ['<rootDir>/src', '<rootDir>/scripts', '<rootDir>/testHelpers'],
  testPathIgnorePatterns: ['/node_modules/'],
  testEnvironment: 'node',
  testRegex: '(/__tests__/.*\\.spec\\.js|(\\.|/)spec)\\.jsx?$',
  watchman: false,
  coverageDirectory: 'coverage',
  coverageReporters: ['text-summary', 'lcov', 'json-summary'],
  collectCoverageFrom: [
    'src/**/*.js',
    '!src/**/*.spec.js',
    '!src/**/__tests__/**',
    '!src/**/fixtures/**',
  ],
  // The floor ratchets to each phase exit's measurement, rounded down to one decimal, so coverage
  // can only go up. Only `npm run test:coverage` enforces it; `test:coverage:unit` passes
  // --coverageThreshold '{}' because it covers a strict subset of `src/`.
  // The three path keys below take their files OUT of the global aggregate, so the global numbers
  // are measured over the remaining files.
  // Re-derived on 2026-10-31 from the 9C exit's two cold coverage runs on Kafka 4.3.1, the lower of
  // the two (global S 98.92 / B 96.62 / F 98.92 / L 99.15). The global floors keep a few items of slack,
  // because the live suite decides some branches by broker timing; the global branch floor keeps
  // ten. A path key whose rounded-down floor would leave no spare item keeps one instead. No floor
  // is lower than before.
  coverageThreshold: {
    global: {
      statements: 98.9,
      branches: 96.4,
      functions: 98.9,
      lines: 99.1,
    },
    // Measured 99.84 / 97.74 / 100.00 / 99.82. The sticky assigner's wide-shape case reaches
    // PartitionMovements' repeat-move bookkeeping; stickyCore.js keeps 4 of its 137 branches
    // uncovered.
    './src/consumer/assigners/': {
      statements: 99.8,
      branches: 97.3,
      functions: 99.3,
      lines: 99.8,
    },
    // Measured 100.00 / 99.57 / 100.00 / 100.00.
    './src/consumer/groupProtocol/': {
      statements: 99.9,
      branches: 99.4,
      functions: 100.0,
      lines: 99.9,
    },
    // Every file of the producer's state is covered by its own unit specs, line for line and
    // branch for branch.
    './src/producer/state/': {
      statements: 100.0,
      branches: 100.0,
      functions: 100.0,
      lines: 100.0,
    },
  },
  bail: false,
}
