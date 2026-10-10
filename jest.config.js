module.exports = {
  verbose: !!process.env.VERBOSE,
  moduleNameMapper: {
    // Anchored: an unanchored key matches any require path that merely contains the name, so a
    // module under testHelpers/ would resolve to the index instead of itself.
    '^testHelpers$': '<rootDir>/testHelpers/index.js',
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
  // The three path keys below take their files OUT of the global aggregate, so the global numbers
  // are re-derived over the remaining files at every exit that changes that population. Re-derived
  // on 2026-10-10 at 3.0.0-alpha.7 (S 97.89 / B 93.06 / F 98.39 / L 98.01): `src/producer/state/`
  // now has its own key, so its files left the global aggregate. That is the re-derivation, not a
  // floor moved down to hide a drop - counted globally as before, the same files score
  // S 98.04 / B 93.79 / F 98.47 / L 98.15, above every earlier global floor.
  // Every floor below is the LOWEST of two full runs, rounded down: the live suite decides some
  // branches by broker timing (groupProtocol/ read 94.58 and 94.81 on its branches). Global
  // branches and lines, and groupProtocol/ branches, sit a step lower still: three runs covered
  // 3846 to 3849 of 4135 global branches (93.0 needs 3846), and the other two had a margin of one.
  coverageThreshold: {
    global: {
      statements: 97.8,
      branches: 92.9,
      functions: 98.3,
      lines: 97.9,
    },
    // Measured 97.45 / 91.24 / 99.15 / 97.24, unchanged from 3.0.0-alpha.6b; nothing touched this
    // tree. Branches and lines sit below the global figures because stickyCore.js keeps
    // PartitionMovements' repeat-move bookkeeping (B 86.29, L 95.92), which no input reaches.
    './src/consumer/assigners/': {
      statements: 97.4,
      branches: 91.2,
      functions: 99.1,
      lines: 97.2,
    },
    // Measured 99.26 / 94.58-94.81 / 100.00 / 99.48; the branch floor takes the lower reading.
    './src/consumer/groupProtocol/': {
      statements: 99.2,
      branches: 94.3,
      functions: 100.0,
      lines: 99.4,
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
