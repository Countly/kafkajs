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
  reporters: ['default', 'jest-junit'],
  coverageDirectory: 'coverage',
  coverageReporters: ['text-summary', 'lcov', 'json-summary'],
  collectCoverageFrom: [
    'src/**/*.js',
    '!src/**/*.spec.js',
    '!src/**/__tests__/**',
    '!src/**/fixtures/**',
  ],
  // Baseline measured 2026-09-03 (full suite, apache/kafka 4.3.1): S 95.62 / B 85.65 / F 97.71 / L 95.64.
  // Floor is floor(baseline) - 1 per metric (decision D21). Only `yarn test:coverage` enforces it;
  // `test:coverage:unit` passes --coverageThreshold '{}' because it covers a strict subset of `src/`.
  coverageThreshold: {
    global: {
      statements: 94,
      branches: 84,
      functions: 96,
      lines: 94,
    },
  },
  bail: false,
}
