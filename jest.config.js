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
  // The two path keys below take their files OUT of the global aggregate, so the global numbers
  // are re-derived over the remaining files at every exit that changes that population. Re-derived
  // on 2026-09-09 at 3.0.0-alpha.6b (S 97.42 / B 91.67 / F 98.07 / L 97.55): `groupProtocol/` gained
  // four files, and because they carry 168 fully covered functions their departure from the global
  // aggregate moves the global functions figure from 98.20 to 98.07. That is the re-derivation, not
  // a regression — the same run scores 98.20 with those files counted globally.
  // Every floor below is the LOWEST of two full runs, rounded down: the live suite decides one
  // branch in groupProtocol/classic.js by timing (63 or 64 of 69), which moves that directory's
  // branch figure between 93.51 and 93.74.
  coverageThreshold: {
    global: {
      statements: 97.4,
      branches: 91.6,
      functions: 98.0,
      lines: 97.5,
    },
    // Measured 97.45 / 91.24 / 99.15 / 97.24, unchanged from 3.0.0-alpha.6a — no 6B task touched
    // this tree. Branches and lines sit below the global figures because stickyCore.js keeps
    // PartitionMovements' repeat-move bookkeeping (B 86.29, L 95.92), which no input reaches.
    './src/consumer/assigners/': {
      statements: 97.4,
      branches: 91.2,
      functions: 99.1,
      lines: 97.2,
    },
    // Measured 99.26 / 93.51-93.74 / 100.00 / 99.48; the branch floor takes the lower reading.
    './src/consumer/groupProtocol/': {
      statements: 99.2,
      branches: 93.5,
      functions: 100.0,
      lines: 99.4,
    },
  },
  bail: false,
}
