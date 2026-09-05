#!/usr/bin/env bash
#
# Prepare a Debian-family WSL distro (Kali or Debian) for YashServer testing.
#
#   sudo bash /mnt/c/projects/yashserver/scripts/wsl_setup.sh
#
# Idempotent: safe to re-run. Installs system packages, then builds a
# virtualenv at ~/yashserver-venv with the test tooling in it.
#
set -uo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "This script installs system packages; re-run it with sudo:" >&2
    echo "  sudo bash $0" >&2
    exit 1
fi

# The invoking user, so the venv is not left owned by root.
TARGET_USER="${SUDO_USER:-root}"
TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6)"
VENV="$TARGET_HOME/yashserver-venv"

. /etc/os-release
echo "=============================================="
echo " YashServer test environment setup"
echo " distro: $PRETTY_NAME"
echo " user  : $TARGET_USER"
echo " venv  : $VENV"
echo "=============================================="
echo

export DEBIAN_FRONTEND=noninteractive

echo "--- apt-get update ---"
apt-get update -qq || { echo "apt-get update failed; check network/repos" >&2; exit 2; }

# Package names verified against Debian 13 (trixie) and Kali rolling, which
# share an archive. Where a name has been superseded, the modern one is used
# and the transitional one is left out.
PACKAGES=(
    # python
    python3 python3-pip python3-venv python3-dev
    # toolchain
    build-essential gcc g++ make pkg-config
    # tls
    openssl libssl-dev ca-certificates
    # fetch / vcs
    git curl wget
    # archives. NOTE: 'unar' is the free RAR *extractor*; see the RAR note
    # at the bottom of this script. Creating RAR needs non-free software and
    # is deliberately not installed here.
    unzip zip tar gzip bzip2 xz-utils unar libarchive-tools
    # inspection
    file procps lsof
    # networking
    iproute2 net-tools iputils-ping bind9-dnsutils
    netcat-openbsd socat tcpdump iperf3 traceroute
)

echo
echo "--- installing ${#PACKAGES[@]} packages ---"
failed=()
for pkg in "${PACKAGES[@]}"; do
    if dpkg -s "$pkg" >/dev/null 2>&1; then
        continue
    fi
    if apt-get install -y -qq "$pkg" >/dev/null 2>&1; then
        echo "  installed $pkg"
    else
        echo "  FAILED    $pkg"
        failed+=("$pkg")
    fi
done
[ ${#failed[@]} -eq 0 ] && echo "  (all present)" || echo "  could not install: ${failed[*]}"

# --- virtualenv -----------------------------------------------------------
# Debian and Kali mark the system Python as externally managed (PEP 668), so
# test tooling has to live in a venv rather than in the system site-packages.
echo
echo "--- virtualenv ---"
if [ ! -x "$VENV/bin/python" ]; then
    sudo -u "$TARGET_USER" python3 -m venv "$VENV" || { echo "venv creation failed" >&2; exit 3; }
    echo "  created $VENV"
else
    echo "  reusing $VENV"
fi

sudo -u "$TARGET_USER" "$VENV/bin/python" -m pip install --quiet --upgrade pip setuptools wheel

# YashServer itself has no runtime dependencies. These are test/dev tools only.
sudo -u "$TARGET_USER" "$VENV/bin/python" -m pip install --quiet \
    pytest pytest-asyncio coverage psutil rarfile \
    && echo "  installed test tooling" \
    || echo "  WARNING: test tooling install failed"

echo
echo "=============================================="
echo " verification"
echo "=============================================="
printf "  %-14s %s\n" "python3" "$(python3 -V 2>&1)"
printf "  %-14s %s\n" "venv python" "$("$VENV/bin/python" -V 2>&1)"
printf "  %-14s %s\n" "pip" "$("$VENV/bin/python" -m pip --version 2>&1 | cut -d' ' -f1-2)"
printf "  %-14s %s\n" "openssl" "$(openssl version 2>&1)"
printf "  %-14s %s\n" "python ssl" "$(python3 -c 'import ssl; print(ssl.OPENSSL_VERSION)' 2>&1)"
echo
echo "  tooling:"
for c in gcc g++ make pkg-config git curl unzip zip bzip2 xz file; do
    printf "    %-12s %s\n" "$c" "$(command -v $c >/dev/null && echo yes || echo MISSING)"
done
echo "  networking:"
for c in ip ss netstat ping dig lsof nc socat tcpdump iperf3 traceroute; do
    printf "    %-12s %s\n" "$c" "$(command -v $c >/dev/null && echo yes || echo MISSING)"
done
echo "  archives:"
for c in unzip zip tar gzip bzip2 xz unar bsdtar; do
    printf "    %-12s %s\n" "$c" "$(command -v $c >/dev/null && echo yes || echo MISSING)"
done

cat <<'NOTE'

----------------------------------------------------------------------
RAR support -- read this before RAR work starts
----------------------------------------------------------------------
RAR is a proprietary format and the split matters:

  EXTRACTING rar  -- supported with free software. Installed here is
                     'unar', which the Python 'rarfile' package drives.
                     No WinRAR, no non-free repo needed.

  CREATING rar    -- NOT possible with free software. The only encoder
                     is the proprietary 'rar' binary (non-free), which
                     this script deliberately does not install.

So YashServer can read/extract RAR on Linux, but cannot produce it
without shipping non-free software. Read support is the realistic goal.
----------------------------------------------------------------------
NOTE

echo
echo "Done. Activate with:  source $VENV/bin/activate"
