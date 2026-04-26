#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <version-without-v> <dist-dir>" >&2
  exit 1
fi

version="$1"
dist_dir="$2"
out_file="${HOMEBREW_FORMULA_OUT:-packaging/homebrew/Formula/clauditor.rb}"
out_dir="$(dirname "$out_file")"
repo="softcane/clauditor"

checksum() {
  local artifact="$1"
  awk '{print $1}' "${dist_dir}/${artifact}.sha256"
}

linux_x86="clauditor-x86_64-unknown-linux-gnu.tar.gz"
linux_arm="clauditor-aarch64-unknown-linux-gnu.tar.gz"
mac_x86="clauditor-x86_64-apple-darwin.tar.gz"
mac_arm="clauditor-aarch64-apple-darwin.tar.gz"

mkdir -p "$out_dir"

cat >"$out_file" <<RUBY
class Clauditor < Formula
  desc "Local fail-open observability proxy for Claude Code"
  homepage "https://github.com/${repo}"
  version "${version}"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/${repo}/releases/download/v${version}/${mac_arm}"
      sha256 "$(checksum "$mac_arm")"
    else
      url "https://github.com/${repo}/releases/download/v${version}/${mac_x86}"
      sha256 "$(checksum "$mac_x86")"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/${repo}/releases/download/v${version}/${linux_arm}"
      sha256 "$(checksum "$linux_arm")"
    else
      url "https://github.com/${repo}/releases/download/v${version}/${linux_x86}"
      sha256 "$(checksum "$linux_x86")"
    end
  end

  def install
    bin.install "clauditor"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/clauditor --version")
  end
end
RUBY

echo "$out_file"
