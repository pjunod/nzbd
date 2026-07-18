# Homebrew formula for nzbd (source build). Host it in a tap
# (e.g. pjunod/homebrew-nzbd) and update url/sha256 per release:
#   brew tap pjunod/nzbd && brew install nzbd
class Nzbd < Formula
  desc "NZBGet-compatible usenet download daemon, reimplemented in Rust"
  homepage "https://github.com/pjunod/nzbd"
  url "https://github.com/pjunod/nzbd/archive/refs/tags/v0.1.0.tar.gz"
  sha256 "REPLACE_WITH_RELEASE_TARBALL_SHA256"
  license "MIT"
  head "https://github.com/pjunod/nzbd.git", branch: "main"

  depends_on "rust" => :build
  depends_on "par2"
  depends_on "sevenzip"

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/nzbd")
  end

  service do
    run [opt_bin/"nzbd", "run", "--config", etc/"nzbd/nzbd.toml"]
    keep_alive true
    log_path var/"log/nzbd.log"
    error_log_path var/"log/nzbd.log"
  end

  test do
    assert_match "nzbd", shell_output("#{bin}/nzbd --help")
  end
end
