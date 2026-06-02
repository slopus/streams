# Agent Instructions

- Do not introduce explicit alternate modes, protocol modes, or separate media/data-plane modes unless the user explicitly asks for them.
- For RAM-only live media work, make the vanilla `durability:"ephemeral"` topic path work well. Preserve monotonic sequence numbers and existing topic contracts.
- Do not use generic topic names like `job` or `jobs` in examples. Use a more specific topic name that reflects the use case.
