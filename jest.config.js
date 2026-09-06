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
  // Baseline measured 2026-09-06 at 3.0.0-alpha.5b (full suite, apache/kafka 4.3.1):
  // S 97.15 / B 90.09 / F 97.96 / L 97.28. The floor ratchets to each phase exit's measurement,
  // rounded down to one decimal, so coverage can only go up. Only `npm run test:coverage` enforces it;
  // `test:coverage:unit` passes --coverageThreshold '{}' because it covers a strict subset of `src/`.
  coverageThreshold: {
    global: {
      statements: 97.1,
      branches: 90,
      functions: 97.9,
      lines: 97.2,
    },
  },
  bail: false,
}
