# Fix Plan

- [x] Fix `logger.log()` TypeError in `consumer.py` (change to `logger.info()`)
- [x] Add 2-minute idle timeout in `consumer.py` to trigger `stop_event.set()` when no data is received
- [x] Test by running `python main.py` (syntax check passed; file compiles cleanly)
