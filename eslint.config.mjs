import { fileURLToPath } from 'node:url'
import js from '@eslint/js'
import globals from 'globals'
import n from 'eslint-plugin-n'
import jest from 'eslint-plugin-jest'
import prettierPlugin from 'eslint-plugin-prettier'
import prettierConfig from 'eslint-config-prettier'
import { includeIgnoreFile } from '@eslint/compat'

export default [
  // Reads .gitignore so lint covers the whole tree without shelling out to `git ls-files`.
  includeIgnoreFile(fileURLToPath(new URL('.gitignore', import.meta.url))),
  {
    ignores: ['node_modules/**', 'coverage/**'],
  },
  js.configs.recommended,
  {
    files: ['**/*.js', '**/*.mjs'],
    languageOptions: {
      ecmaVersion: 2023,
      sourceType: 'commonjs',
      globals: { ...globals.node, ...globals.jest },
    },
    plugins: { n, jest, prettier: prettierPlugin },
    rules: {
      ...n.configs['flat/recommended-script'].rules,
      ...prettierConfig.rules,
      'prettier/prettier': 'error',

      // --- ported from eslint-config-standard@17.1.0 -------------------------------------
      // The 52 core rules `standard` sets that @eslint/js `recommended` does not (minus the 47
      // eslint-config-prettier disables). 50 are listed here; `camelcase` is in the options
      // block below (merged with this repo's ^testIfKafka_ allowance), and `no-new-symbol` is
      // deliberately dropped: it is deprecated in ESLint 9 and superseded by
      // `no-new-native-nonconstructor`, which `js.configs.recommended` already sets to 'error'.
      // See the task header for the measurement.
      'no-var': 'warn',
      'object-shorthand': ['warn', 'properties'],
      'accessor-pairs': ['error', { setWithoutGet: true, enforceForClassMembers: true }],
      'array-callback-return': ['error', { allowImplicit: false, checkForEach: false }],
      'default-case-last': 'error',
      'dot-notation': ['error', { allowKeywords: true }],
      eqeqeq: ['error', 'always', { null: 'ignore' }],
      'lines-between-class-members': ['error', 'always', { exceptAfterSingleLine: true }],
      'new-cap': ['error', { newIsCap: true, capIsNew: false, properties: true }],
      'no-array-constructor': 'error',
      'no-caller': 'error',
      'no-eval': 'error',
      'no-extend-native': 'error',
      'no-extra-bind': 'error',
      'no-implied-eval': 'error',
      'no-iterator': 'error',
      'no-labels': ['error', { allowLoop: false, allowSwitch: false }],
      'no-lone-blocks': 'error',
      'no-multi-str': 'error',
      'no-new': 'error',
      'no-new-func': 'error',
      'no-new-object': 'error',
      'no-new-wrappers': 'error',
      'no-octal-escape': 'error',
      'no-proto': 'error',
      'no-return-assign': ['error', 'except-parens'],
      'no-self-compare': 'error',
      'no-sequences': 'error',
      'no-template-curly-in-string': 'error',
      'no-throw-literal': 'error',
      'no-undef-init': 'error',
      'no-unmodified-loop-condition': 'error',
      'no-unneeded-ternary': ['error', { defaultAssignment: false }],
      'no-unreachable-loop': 'error',
      'no-unused-expressions': [
        'error',
        { allowShortCircuit: true, allowTernary: true, allowTaggedTemplates: true },
      ],
      'no-use-before-define': ['error', { functions: false, classes: false, variables: false }],
      'no-useless-call': 'error',
      'no-useless-computed-key': 'error',
      'no-useless-constructor': 'error',
      'no-useless-rename': 'error',
      'no-useless-return': 'error',
      'no-void': 'error',
      'one-var': ['error', { initialized: 'never' }],
      'prefer-const': ['error', { destructuring: 'all' }],
      'prefer-promise-reject-errors': 'error',
      'prefer-regex-literals': ['error', { disallowRedundantWrapping: true }],
      'spaced-comment': [
        'error',
        'always',
        {
          line: { markers: ['*package', '!', '/', ',', '='] },
          block: {
            balanced: true,
            markers: ['*package', '!', ',', ':', '::', 'flow-include'],
            exceptions: ['*'],
          },
        },
      ],
      'symbol-description': 'error',
      'unicode-bom': ['error', 'never'],
      yoda: ['error', 'never'],

      // --- standard's option objects for rules `recommended` also sets ---------------------
      // Stated explicitly rather than inherited: ESLint 9 changed `no-unused-vars` defaults
      // to caughtErrors:'all' / args:'after-used', which would flag the six intentional
      // `catch (e) {}` / `catch (_)` sites in src/ (brokerPool.js:277,:299,
      // runner.js:189, broker/index.js:100, consumer/index.js:345,:401).
      'no-unused-vars': [
        'error',
        { args: 'none', caughtErrors: 'none', ignoreRestSiblings: true, vars: 'all' },
      ],
      'no-empty': ['error', { allowEmptyCatch: true }],
      'no-constant-condition': ['error', { checkLoops: false }],
      'no-redeclare': ['error', { builtinGlobals: false }],
      'no-self-assign': ['error', { props: true }],
      'use-isnan': ['error', { enforceForSwitchCase: true, enforceForIndexOf: true }],
      'valid-typeof': ['error', { requireStringLiterals: true }],
      // standard's options plus this repo's testHelpers allowance (was in .eslintrc).
      // Deviates from the brief's literal `^testIfKafka_` (requires an underscore right after
      // the prefix): the actual identifiers are `testIfKafkaAtMost_0_10`,
      // `testIfKafkaAtLeast_0_11`, `testIfKafkaAtLeast_1_1_0` (testHelpers/index.js:402-404) —
      // no underscore directly follows "testIfKafka". `^testIfKafka` restores the old
      // `.eslintrc` behavior (`^testIfKafka_*`, which — with no `$` anchor and `_*` meaning
      // "zero or more underscores" — matched any identifier merely prefixed "testIfKafka").
      camelcase: [
        'error',
        { allow: ['^UNSAFE_', '^testIfKafka'], properties: 'never', ignoreGlobals: true },
      ],

      // --- carried over from .eslintrc (Phase 1 baseline) ---------------------------------
      'no-unexpected-multiline': 'off',
      'no-return-await': 'off',
      'n/no-deprecated-api': 'off',
      'n/no-process-exit': 'off',
      'n/hashbang': 'off',
      'n/no-missing-require': ['error', { allowModules: ['testHelpers'] }],

      'jest/no-alias-methods': 'error',
      'jest/no-focused-tests': 'error',
      'jest/no-commented-out-tests': 'error',
      'jest/no-deprecated-functions': 'error',
      'jest/no-jasmine-globals': 'error',
      // 'jest/no-jest-import' was removed from eslint-plugin-jest (absent in 29.16.6): the rule
      // flagged `import jest from 'jest'`, an anti-pattern from pre-globals-injection Jest that
      // no longer applies. Nothing to port.
      'jest/no-test-prefixes': 'error',
      'jest/valid-describe-callback': 'error',
      'jest/valid-expect': 'error',
      'jest/valid-expect-in-promise': 'error',
    },
  },
  {
    files: ['**/*.mjs'],
    languageOptions: { sourceType: 'module' },
    rules: {
      // The block above applies n's CommonJS ("script") preset to *.mjs as well, so ESM syntax
      // trips n/no-unsupported-features/es-syntax on index.mjs and scripts/packageSmoke.test.mjs.
      // The .eslintrc this replaces carried the same override (`.eslintrc:57`,
      // "node/no-unsupported-features/es-syntax": "off") for exactly this reason.
      'n/no-unsupported-features/es-syntax': 'off',
    },
  },
  {
    files: ['src/producer/partitioners/legacy/randomBytes.js'],
    rules: {
      // This file feature-detects `global.crypto` (the WebCrypto global) with a fallback to
      // `require('crypto')`. Node's own stability index keeps that global at "Experimental"
      // until 23.0.0 (eslint-plugin-n's data confirms this), even though it has been enabled
      // by default since 19.0.0 and works fine across this repo's >=22.0.0 engines range —
      // the feature-detection here is deliberate, not a bug to fix.
      'n/no-unsupported-features/node-builtins': 'off',
    },
  },
  {
    files: ['testHelpers/setup.js'],
    rules: {
      // `fs.globSync` isn't backported until Node 22.17.0 (eslint-plugin-n's data), below this
      // package's published `engines.node` floor of >=22.0.0. That floor describes the runtime
      // for consumers of `index.js`/`index.mjs`; this file is dev-only test tooling (excluded
      // from the npm package via .npmignore) run against the Node versions actually in the CI
      // matrix and used locally, all well above 22.17.
      'n/no-unsupported-features/node-builtins': 'off',
    },
  },
]
