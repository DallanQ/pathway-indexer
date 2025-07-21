.PHONY: install
install: ## Install the poetry environment and pre-commit hooks
	@echo "🚀 Creating virtual environment using pyenv and poetry"
	@poetry install
	@poetry run pre-commit install
	@echo "✅ Poetry environment and hooks are set up."

# Use this if `poetry shell` fails (e.g., on some Linux dev machines with Poetry ≥2.0)
# It's a workaround to activate the virtual environment manually.
.PHONY: activate
activate:
	# Poetry 2.x no longer includes `poetry shell` by default, so you can use this instead.
	@echo "🌀 To activate the environment, run:"
	@echo "source .venv/bin/activate"

.PHONY: check
check: ## Run code quality tools
	@echo "🚀 Checking Poetry lock file consistency with 'pyproject.toml': Running poetry check --lock"
	@poetry check --lock
	@echo "🚀 Linting code: Running pre-commit"
	@poetry run pre-commit run -a
	@echo "🚀 Static type checking: Running mypy"
	@poetry run mypy

.PHONY: test
test: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@poetry run pytest --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: push-data
push-data: ## Push data up to shared file system
	@echo "🚀 Rsync data directory up to shared file system"
	rsync -avzh data/* ec2-user@35.90.214.49:/interns/pathway

.PHONY: pull-data
pull-data: ## Pull data down from shared file system
	@echo "🚀 Rsync data directory down from shared file system"
	rsync -avzh 'ec2-user@35.90.214.49:/interns/pathway/*' data/

.PHONY: build
build: clean-build ## Build wheel file using poetry
	@echo "🚀 Creating wheel file"
	@poetry build

.PHONY: clean-build
clean-build: ## Clean build artifacts
	@rm -rf dist

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
