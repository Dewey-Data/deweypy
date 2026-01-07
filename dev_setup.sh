#!/usr/bin/env bash

set -euo pipefail

uv sync --group dev

source .venv/bin/activate

# Use `AGENTS.md` instead of `CLAUDE.md`.
# Create a symlink only if `CLAUDE.md` doesn't already exist.
if [ ! -e CLAUDE.md ]; then
    ln -s AGENTS.md CLAUDE.md
    echo "Created symlink: CLAUDE.md -> AGENTS.md"
else
    echo "Skipping symlink creation: CLAUDE.md already exists"
fi

echo ""
echo "Development environment setup complete!"
echo "Virtual environment activated. You can now run your Python scripts."
