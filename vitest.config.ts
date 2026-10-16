import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    globalSetup: ["test/helpers/global-setup.ts"],
    testTimeout: 60_000,
    hookTimeout: 120_000,
    // Tests share one database and use distinct scopes/ids, but the webhook
    // delivery worker claims any due row, so keep files sequential.
    fileParallelism: false,
  },
});
