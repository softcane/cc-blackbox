#!/usr/bin/env sh
set -eu

repo="softcane/cc-blackbox"
bin_name="cc-blackbox"
install_dir="${CC_BLACKBOX_INSTALL_DIR:-$HOME/.local/bin}"
version="${CC_BLACKBOX_VERSION:-latest}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

detect_target() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Darwin)
      os_part="apple-darwin"
      ;;
    Linux)
      os_part="unknown-linux-gnu"
      ;;
    *)
      echo "error: unsupported OS: $os" >&2
      exit 1
      ;;
  esac

  case "$arch" in
    x86_64 | amd64)
      arch_part="x86_64"
      ;;
    arm64 | aarch64)
      arch_part="aarch64"
      ;;
    *)
      echo "error: unsupported architecture: $arch" >&2
      exit 1
      ;;
  esac

  printf '%s-%s' "$arch_part" "$os_part"
}

download() {
  url="$1"
  out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$out"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$out"
  else
    echo "error: curl or wget is required" >&2
    exit 1
  fi
}

sha256_file() {
  file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    echo "error: sha256sum or shasum is required" >&2
    exit 1
  fi
}

need tar
need mktemp
need install
need awk

target="$(detect_target)"
archive="cc-blackbox-${target}.tar.gz"
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

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

echo "Downloading ${archive}..."
download "$artifact_url" "$tmp_dir/$archive"
download "$checksum_url" "$tmp_dir/$archive.sha256"

expected="$(awk '{print $1}' "$tmp_dir/$archive.sha256")"
actual="$(sha256_file "$tmp_dir/$archive")"
if [ "$expected" != "$actual" ]; then
  echo "error: checksum mismatch for ${archive}" >&2
  echo "expected: $expected" >&2
  echo "actual:   $actual" >&2
  exit 1
fi

tar -xzf "$tmp_dir/$archive" -C "$tmp_dir"
mkdir -p "$install_dir"
install -m 0755 "$tmp_dir/$bin_name" "$install_dir/$bin_name"

echo "Installed $bin_name to $install_dir/$bin_name"
case ":$PATH:" in
  *":$install_dir:"*) ;;
  *)
    echo "Add this to your shell PATH if needed:"
    echo "  export PATH=\"$install_dir:\$PATH\""
    ;;
esac

echo
echo "Try:"
echo "  cc-blackbox doctor"
