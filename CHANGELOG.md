# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

- Replace FIFO job queue with Round Robin queue, to prevent large requests starving small requests.

## [0.12.0] - 2026-05-07

- Add rust bindings.
- Bump cffi dep to 2.0.

## [0.11.1] - 2026-05-04

- Skip request parsing by default.
- Tidy up code to have less scattered configuration options.

## [0.11.0] - 2026-04-13

- GJ-63 Always perform mars request expansion on C API / pygribjump.
- GJ-62 Migrate pytests to new pyfdb API.

## [0.10.4] - 2026-03-17

- Remove deprecated metkit MarsExpansion.
- Use metkit's new codes handle interface.
- In tests, set eccodes missing value explicitly to 9999.

## [0.10.3] - 2026-01-09

- Improvements to logging and some changes to python deps.

## [0.10.2] - 2025-08-08

- Support scanning of GRIB files ending in an incomplete message.

## [0.10.1] - 2025-07-16

- Improve scan-files tool to merge into existing indexes.

## [0.10.0] - 2025-04-09

- Add ExtractionIterator API.

## [0.9.3] - 2025-03-24

- Fix callback_vs_scan test.

## [0.9.2] - 2025-03-17

- Enable use of multiple short-lived FDB objects in parallel.
- Implement faster jumpinfo generation for ccsds fields.
- Tidy up of ccsds handling code.

## [0.9.1] - 2025-02-10

- Support year/month/date in fdb list requests.

## [0.9.0] - 2025-01-31

- Add Remote FDB compatibility.

## [0.8.1] - 2024-12-09

- Fix error reporting when forwarding requests and add better task status.

## [0.8.0] - 2024-11-27

- LRU cache now stores jumpinfo per field, instead of per file.
- Add ability to forward scan requests to from one gribjump server to another.
- Improve performance of extraction tasks and serialisation for requests involving many ranges.

## [0.7.1] - 2024-11-14

- Minor log/error reporting fixes.

## [0.7.0] - 2024-11-13

- Optimisations to request handling.

## [0.6.3] - 2024-11-04

- Update python setup, axes, scan-files and metrics.

## [0.6.2] - 2024-10-18

- Improve metrics, plugin manifest, python.

## [0.6.1] - 2024-10-08

- Improve multiserver configuration options.

## [0.6.0] - 2024-10-07

- Add multiserver support.

## [0.5.4] - 2024-09-18

- Improve logging and add context.

## [0.5.3] - 2024-09-16

- Add Callbacks using const fieldlocation.

## [0.5.2] - 2024-09-13

- No changes recorded.

## [0.5.1] - 2024-09-05

- Add DHS-style logging.

## [0.5.0] - 2024-08-12

- Update configuration handling.

## [0.4.1] - 2024-07-25

-  No changes documented.

## [0.4.0] - 2024-05-17

- Refactor code.

## [0.3.0] - 2024-03-01

- Refactor code.

## [0.2.0] - 2024-01-25

- Refactor code.

## [0.1.0] - 2024-01-10

- Migrate GribJump from Metkit.

[Unreleased]: https://github.com/ecmwf/gribjump/compare/0.12.0...HEAD
[0.12.0]: https://github.com/ecmwf/gribjump/compare/0.11.1...0.12.0
[0.11.1]: https://github.com/ecmwf/gribjump/compare/0.11.0...0.11.1
[0.11.0]: https://github.com/ecmwf/gribjump/compare/0.10.4...0.11.0
[0.10.4]: https://github.com/ecmwf/gribjump/compare/0.10.3...0.10.4
[0.10.3]: https://github.com/ecmwf/gribjump/compare/0.10.2...0.10.3
[0.10.2]: https://github.com/ecmwf/gribjump/compare/0.10.1...0.10.2
[0.10.1]: https://github.com/ecmwf/gribjump/compare/0.10.0...0.10.1
[0.10.0]: https://github.com/ecmwf/gribjump/compare/0.9.3...0.10.0
[0.9.3]: https://github.com/ecmwf/gribjump/compare/0.9.2...0.9.3
[0.9.2]: https://github.com/ecmwf/gribjump/compare/0.9.1...0.9.2
[0.9.1]: https://github.com/ecmwf/gribjump/compare/0.9.0...0.9.1
[0.9.0]: https://github.com/ecmwf/gribjump/compare/0.8.1...0.9.0
[0.8.1]: https://github.com/ecmwf/gribjump/compare/0.8.0...0.8.1
[0.8.0]: https://github.com/ecmwf/gribjump/compare/0.7.1...0.8.0
[0.7.1]: https://github.com/ecmwf/gribjump/compare/0.7.0...0.7.1
[0.7.0]: https://github.com/ecmwf/gribjump/compare/0.6.3...0.7.0
[0.6.3]: https://github.com/ecmwf/gribjump/compare/0.6.2...0.6.3
[0.6.2]: https://github.com/ecmwf/gribjump/compare/0.6.1...0.6.2
[0.6.1]: https://github.com/ecmwf/gribjump/compare/0.6.0...0.6.1
[0.6.0]: https://github.com/ecmwf/gribjump/compare/0.5.4...0.6.0
[0.5.4]: https://github.com/ecmwf/gribjump/compare/0.5.3...0.5.4
[0.5.3]: https://github.com/ecmwf/gribjump/compare/0.5.2...0.5.3
[0.5.2]: https://github.com/ecmwf/gribjump/compare/0.5.1...0.5.2
[0.5.1]: https://github.com/ecmwf/gribjump/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/ecmwf/gribjump/compare/0.4.1...0.5.0
[0.4.1]: https://github.com/ecmwf/gribjump/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/ecmwf/gribjump/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/ecmwf/gribjump/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/ecmwf/gribjump/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/ecmwf/gribjump/releases/tag/0.1.0
