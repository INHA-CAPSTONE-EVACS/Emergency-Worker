'use strict';
const transform = {'^.+\\.ts$': ['ts-jest', {tsconfig: '<rootDir>/tsconfig.json'}]};
module.exports = {
  maxWorkers: 1,
  projects: [
    {displayName: 'unit', testEnvironment: 'node', testMatch: ['<rootDir>/test/unit/**/*.test.ts'], transform},
    {displayName: 'integration', testEnvironment: 'node', testMatch: ['<rootDir>/test/integration/**/*.test.ts', '<rootDir>/test/integration/**/*.e2e-spec.ts'], transform},
  ],
  collectCoverageFrom: ['src/**/*.ts'],
};
