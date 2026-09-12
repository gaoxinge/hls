import tseslint from "typescript-eslint";
export default tseslint.config(
  {
    ignores: [
      "test-results/**",
      "tests/fixtures/**",
      "venv/**",
      ".output/**",
      ".wxt/**",
      "node_modules/**",
    ],
  },
  ...tseslint.configs.recommended,
  {
    files: [
      "src/domain/**/*.ts",
      "src/protocol/**/*.ts",
      "src/planning/**/*.ts",
    ],
    rules: {
      "no-restricted-globals": [
        "error",
        "chrome",
        "navigator",
        "indexedDB",
        "fetch",
      ],
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: [
                "**/storage/**",
                "**/adapters/**",
                "**/network/**",
                "**/ui/**",
                "**/bootstrap/**",
              ],
              message: "Domain/protocol/planning must use injected ports.",
            },
          ],
        },
      ],
    },
  },
  {
    files: ["src/ui/**/*.ts"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: [
                "**/storage/**",
                "**/media/**",
                "**/network/**",
                "**/execution/**",
                "**/download/**",
              ],
              message: "UI must use application commands.",
            },
          ],
        },
      ],
    },
  },
  {
    files: ["**/*.mjs"],
    rules: { "@typescript-eslint/no-require-imports": "off" },
  },
);
