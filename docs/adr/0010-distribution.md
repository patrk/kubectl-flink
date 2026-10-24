# 10. Distribution: own krew index first

Releases are built by goreleaser from tags for Linux, macOS and Windows on amd64 and arm64. Each release updates a
custom krew index, so `kubectl krew index add` and `kubectl krew install` work without waiting for review. The same
release ships a krew manifest and archives for offline installation on jumphosts without internet access, and a
Homebrew tap.

The plugin is then submitted to krew-index under the name `flink` for discoverability, with krew-release-bot keeping
it current. If reviewers ask for a narrower name, the krew name becomes `flink-operator` and the repository name stays.
