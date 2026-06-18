# Contributing to lite-task-dispatcher

Thanks for considering contributing! Here's how to get involved.

## Development Setup

```bash
# 1. Fork and clone
git clone https://github.com/<your-username>/lite-task-dispatcher.git
cd lite-task-dispatcher

# 2. Start infrastructure
make infra

# 3. Build
make build

# 4. Run
make run
```

## Branch Strategy

- `master` — stable, release-ready code
- `feat/<name>` — new features
- `fix/<name>` — bug fixes
- `refactor/<name>` — code improvements without behavior change

## Pull Request Process

1. Create a feature branch from `master`
2. Keep commits focused and atomic
3. Ensure all tests pass: `mvn verify`
4. Update documentation if adding new features
5. Open a PR with a clear description of what and why

## Code Style

- Follow existing project conventions
- Use meaningful variable and method names
- Add JavaDoc on public APIs
- Keep methods short and focused

## Reporting Issues

- Use GitHub Issues with the provided templates
- Include steps to reproduce for bugs
- For feature requests, describe the use case

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
