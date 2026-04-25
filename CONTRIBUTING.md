# Contributing to xk6-kafka-rest

Thank you for your interest in contributing! This guide covers everything you need to get started.

---

## Development Setup

**Prerequisites**
- Go 1.21+
- [`xk6`](https://github.com/grafana/xk6): `go install go.k6.io/xk6/cmd/xk6@latest`
- Docker (for the local test stack)

**Clone and build**
```bash
git clone https://github.com/Waleed2660/xk6-kafka-rest.git
cd xk6-kafka-rest
go mod tidy
xk6 build --with github.com/Waleed2660/xk6-kafka-rest=.
```

**Start the local stack**
```bash
cd local-dev
docker compose up -d
# Kafbat UI → http://localhost:8090
```

**Run tests**
```bash
go test -race ./...
```

**Run a smoke test**
```bash
./k6 run local-dev/test-script.js
```

---

## Making Changes

1. Fork the repository and create a branch: `git checkout -b feat/my-feature`
2. Make your changes — keep commits small and focused
3. Add or update tests in `*_test.go` files
4. Run `go vet ./...` and `go test -race ./...` — both must pass
5. Update `CHANGELOG.md` under `[Unreleased]`
6. Open a Pull Request against `main`

### Commit message convention

```
feat: add support for X
fix: handle edge case in Y
docs: update API reference
chore: bump dependencies
```

---

## Running the Full CI Check Locally

```bash
go vet ./...
go test -race -count=1 ./...
xk6 build --with github.com/Waleed2660/xk6-kafka-rest=.
./k6 version
```

---

## Reporting Issues

Please open a [GitHub Issue](https://github.com/Waleed2660/xk6-kafka-rest/issues) with:
- A minimal reproduction script
- The k6 version (`./k6 version`)
- The REST Proxy version you're targeting
- The full error message

---

## License

By contributing you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).

