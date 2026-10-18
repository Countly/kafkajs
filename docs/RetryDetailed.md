---
id: retry-detailed
title: Retry Mechanism Explained
---

The retry mechanism uses a randomization function that grows exponentially. This formula and how the default values affect it is best described by the example below:

- First retry: `ceil(Random(initialRetryTime * (1 - factor), initialRetryTime * (1 + factor)))`, clamped to zero or above.
  With the shared defaults (`initialRetryTime = 300`, `factor = 0.2`), the delay is 240–360 ms.
- Second retry: `min(maxRetryTime, ceil(Random(previous * (1 - factor), previous * (1 + factor))) * multiplier)`, clamped to zero or above.
  With `multiplier = 2`, the possible delay is 384–864 ms.
- Third retry: the same recurrence gives 616–2074 ms. The ceilings are applied before multiplication, so the lower bound is 616 ms, rather than 614.4 ms.
- Later delays use the same recurrence and the shared `maxRetryTime = 30000` ms cap. These are ranges across all random draws, not a promise that every run visits each endpoint.

These values describe the shared retry defaults. Explicit retry options override them. Producer-specific defaults must be checked separately when they change.
