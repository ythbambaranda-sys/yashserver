# About yashserver

`yashserver` is an independent Python server library focused on making server development simple and practical.

It gives you:
- TCP servers
- HTTP servers
- WebSocket servers
- Plugin support
- Auth and rate limiting
- Optional TLS
- Optional database connectors

The project is designed for developers who want fast setup and clean code, without needing to build everything from scratch.

## Package Notes

- PyPI package name: `yashserver`
- Main source package: `src/yashserver`
- Compatibility import path: `src/yserver`

This means both imports can work:

```python
import yserver
# or
import yashserver
```

## Goal

Provide an easy, beginner-friendly server toolkit that is still strong enough for real projects.
