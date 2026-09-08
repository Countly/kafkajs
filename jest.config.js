module.exports = {
  verbose: !!process.env.VERBOSE,
  moduleNameMapper: {
    testHelpers: '<rootDir>/testHelpers/index.js',
  },
  setupFilesAfterEnv: ['<rootDir>/testHelpers/setup.js'],
  // Only these trees hold specs; keeps stray local directories out of test discovery.
  roots: ['<rootDir>/src', '<rootDir>/scripts'],
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
  // The two path keys below take their files OUT of the global aggregate, so the global numbers
  // were re-derived over the remaining files on 2026-09-08 at 3.0.0-alpha.6a (S 97.24 / B 90.51 /
  // F 98.13 / L 97.36) rather than carried over from the 3.0.0-alpha.5b measurement.
  coverageThreshold: {
    global: {
      statements: 97.2,
      branches: 90.5,
      functions: 98.1,
      lines: 97.3,
    },
    // Measured 97.45 / 91.24 / 99.15 / 97.24. Lines sit 0.12 below the global figure because
    // stickyCore.js keeps PartitionMovements' repeat-move bookkeeping, which no input reaches.
    './src/consumer/assigners/': {
      statements: 97.4,
      branches: 91.2,
      functions: 99.1,
      lines: 97.2,
    },
    // Measured 98.24 / 90.79 / 100.00 / 98.18.
    './src/consumer/groupProtocol/': {
      statements: 98.2,
      branches: 90.7,
      functions: 100.0,
      lines: 98.1,
    },
  },
  bail: false,
}
