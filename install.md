# AI Agent Install Guide for cc-blackbox

This guide is written for an AI agent that can run shell commands on behalf of a user. It mirrors `install.sh`, but spells out the checks, choices, and verification steps so the setup can be done safely without guessing.

## Goal

Install the `cc-blackbox` CLI from the GitHub release artifact, verify the checksum, make sure it is reachable on `PATH`, and validate the local guard stack.

Do not edit project source files for a normal user install. Use the release artifact unless the user explicitly asks for a source build.

Run shell snippets in a POSIX-compatible shell. When using the manual path, keep the commands in the same shell session so variables such as `archive`, `artifact_url`, and `tmp_dir` remain available.

## Supported Targets

`install.sh` supports these targets:

| OS | Architecture | Release artifact target |
| --- | --- | --- |
| macOS | Intel | `x86_64-apple-darwin` |
| macOS | Apple Silicon | `aarch64-apple-darwin` |
| Linux | x86_64 / amd64 | `x86_64-unknown-linux-gnu` |
| Linux | arm64 / aarch64 | `aarch64-unknown-linux-gnu` |

Stop and report the exact unsupported OS or architecture if the machine is not one of these.

## Prerequisites

Check these commands before installing:

```sh
for cmd in uname tar mktemp install awk; do
  command -v "$cmd" >/dev/null || { echo "missing required command: $cmd"; exit 1; }
done

command -v curl >/dev/null || command -v wget >/dev/null || {
  echo "missing required command: curl or wget"
  exit 1
}

command -v sha256sum >/dev/null || command -v shasum >/dev/null || {
  echo "missing required command: sha256sum or shasum"
  exit 1
}
```

For guard mode setup, also check Docker:

```sh
if docker compose version >/dev/null 2>&1; then
  echo "docker compose is available"
elif docker-compose version >/dev/null 2>&1; then
  echo "docker-compose is available"
else
  echo "Docker Compose is not available"
fi
```

If Docker or Docker Compose is missing, the CLI can still be installed, but `cc-blackbox guard start` will not be able to start the local proxy stack.

## Inputs

Use these defaults unless the user asks for something else:

```sh
export CC_BLACKBOX_INSTALL_DIR="${CC_BLACKBOX_INSTALL_DIR:-$HOME/.local/bin}"
export CC_BLACKBOX_VERSION="${CC_BLACKBOX_VERSION:-latest}"
```

`CC_BLACKBOX_VERSION=latest` installs the latest GitHub release. A specific version may be provided as `v1.2.3` or `1.2.3`.

## Fast Path

The official installer is the preferred release install path. Run:

```sh
curl -fsSL https://raw.githubusercontent.com/softcane/cc-blackbox/main/install.sh | sh
```

Then continue with [Verify the Install](#verify-the-install).

## Manual Equivalent

Use this path when the user wants the steps performed explicitly or when you need to debug an install.

1. Detect the release target:

```sh
set -eu

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Darwin) os_part="apple-darwin" ;;
  Linux) os_part="unknown-linux-gnu" ;;
  *) echo "unsupported OS: $os"; exit 1 ;;
esac

case "$arch" in
  x86_64|amd64) arch_part="x86_64" ;;
  arm64|aarch64) arch_part="aarch64" ;;
  *) echo "unsupported architecture: $arch"; exit 1 ;;
esac

target="${arch_part}-${os_part}"
archive="cc-blackbox-${target}.tar.gz"
```

2. Build the release URLs:

```sh
repo="softcane/cc-blackbox"
version="${CC_BLACKBOX_VERSION:-latest}"
base_url="https://github.com/${repo}/releases"

if [ "$version" = "latest" ]; then
  artifact_url="${base_url}/latest/download/${archive}"
  checksum_url="${base_url}/latest/download/${archive}.sha256"
else
  case "$version" in
    v*) release_tag="$version" ;;
    *) release_tag="v$version" ;;
  esac
  artifact_url="${base_url}/download/${release_tag}/${archive}"
  checksum_url="${base_url}/download/${release_tag}/${archive}.sha256"
fi
```

3. Download the archive and checksum:

```sh
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

download_with_curl_or_wget() {
  url="$1"
  out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$out"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$out"
  else
    echo "curl or wget is required"
    exit 1
  fi
}

download_with_curl_or_wget "$artifact_url" "$tmp_dir/$archive"
download_with_curl_or_wget "$checksum_url" "$tmp_dir/$archive.sha256"
```

4. Verify the checksum before extracting:

```sh
expected="$(awk '{print $1}' "$tmp_dir/$archive.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$tmp_dir/$archive" | awk '{print $1}')"
else
  actual="$(shasum -a 256 "$tmp_dir/$archive" | awk '{print $1}')"
fi

if [ "$expected" != "$actual" ]; then
  echo "checksum mismatch for ${archive}"
  echo "expected: $expected"
  echo "actual:   $actual"
  exit 1
fi
```

5. Install the binary:

```sh
install_dir="${CC_BLACKBOX_INSTALL_DIR:-$HOME/.local/bin}"
mkdir -p "$install_dir"
tar -xzf "$tmp_dir/$archive" -C "$tmp_dir"
install -m 0755 "$tmp_dir/cc-blackbox" "$install_dir/cc-blackbox"
```

## Verify the Install

Resolve the installed binary. Prefer `cc-blackbox` on `PATH`, otherwise use the absolute install path:

```sh
install_dir="${CC_BLACKBOX_INSTALL_DIR:-$HOME/.local/bin}"
if command -v cc-blackbox >/dev/null 2>&1; then
  cc_blackbox_bin="cc-blackbox"
else
  cc_blackbox_bin="$install_dir/cc-blackbox"
fi
```

Check that the binary exists and can run:

```sh
"$cc_blackbox_bin" --version
"$cc_blackbox_bin" --help
```

If `cc-blackbox` is not available by name, check whether the install directory is on `PATH`:

```sh
case ":$PATH:" in
  *":${CC_BLACKBOX_INSTALL_DIR:-$HOME/.local/bin}:"*) echo "install directory is on PATH" ;;
  *) echo "add this to the user's shell profile: export PATH=\"${CC_BLACKBOX_INSTALL_DIR:-$HOME/.local/bin}:\$PATH\"" ;;
esac
```

Do not silently edit shell startup files unless the user asked you to. If you do edit one, inspect the existing file first and make the smallest append-only change.

## Validate the Guard Stack

Run:

```sh
"$cc_blackbox_bin" doctor
```

If Docker and Docker Compose are available and the user wants the local proxy stack started, run:

```sh
"$cc_blackbox_bin" guard start
"$cc_blackbox_bin" guard policy
"$cc_blackbox_bin" guard status
```

The local services bind to `127.0.0.1` by default. Useful checks:

```sh
curl -s http://127.0.0.1:9091/api/summary
curl -s http://127.0.0.1:9091/metrics | head
```

## Run Claude Code Through cc-blackbox

When the guard stack is running, start Claude Code through the proxy:

```sh
"$cc_blackbox_bin" run claude
```

To observe guard findings in another terminal:

```sh
"$cc_blackbox_bin" guard watch
```

After a session has enough evidence, read the latest postmortem:

```sh
"$cc_blackbox_bin" postmortem latest
```

## Troubleshooting

- `unsupported OS` or `unsupported architecture`: the release installer does not have an artifact for this machine. Stop and report the detected values from `uname -s` and `uname -m`.
- `checksum mismatch`: delete the temporary directory, re-download once, and stop if the mismatch repeats.
- `cc-blackbox: command not found`: run the binary by absolute path and tell the user to add the install directory to `PATH`.
- Docker errors from `guard start`: install/start Docker Desktop on macOS or Docker Engine with Compose on Linux, then rerun `cc-blackbox guard start`.
- Port conflicts: inspect the error from `cc-blackbox doctor` or `cc-blackbox guard start` before changing ports or stopping processes.

## Agent Completion Report

When done, report:

- installed path;
- installed version from `cc-blackbox --version`;
- whether the install directory is on `PATH`;
- result of `cc-blackbox doctor`;
- whether the guard stack was started;
- any action the user must take manually, especially Docker setup or shell `PATH` changes.
