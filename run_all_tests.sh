#!/bin/bash

BLACK='\033[0;30m'
RED='\033[0;31m'
GREEN='\033[0;32m'
RESET='\033[0m'

tags=""
flags=""
debug_flags=""
short="-short"       # run only short tests per default
run_reg="true"       # run regular tests per default

if [ -n "${SET_DEBUG_OUTPUT:-}" ]; then
    debug_flags="-v -x"
fi

# set limit of open files
#ulimit -Sn 1024

if [[ -z "$TMPDIR" ]]; then
    TMPDIR="/tmp"
fi
out=$(mktemp "$TMPDIR/run_all_tests.XXXXXX")
ret_global=0

function usage() {
    echo "usage: $0 [-tags \"TAGS\"] "
    echo
    echo "run regular tests"
    echo -e "${RESET}"
}

function handleResult() {
    if [[ $1 == 0 ]]; then
        echo -e "\n${GREEN}ALL TESTS SUCCEEDED"
    else
        ret_global=1
        echo -e "\n${RED}SOME TESTS FAILED:"
        cat "${out}" | grep -aE "FAIL	|--- FAIL"
    fi

    rm -f "${out}"
    echo -e "${RESET}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t | -tags | --tags)
            tags="$2"
            shift
            shift
            ;;
        -s | -sequential | --sequential)
            flags="-parallel 1"
            shift
            ;;
        -v | --verbose)
            debug_flags="-v"
            shift
            ;;
        -h | -help | --help)
            usage
            exit
            ;;
        *)
            echo "unknown option \"$1\""
            usage
            exit
            ;;
    esac
done

#go generate ./version

# show version info
#grep -E "revision|branch"  version/version-info.go

if [[ "${run_reg}" == "true" ]]; then
    echo "running regular tests"

    ret=0
    go test ${debug_flags} -timeout 3m $flags -tags "$tags" $short -count=1 ./... 2>&1 |
        tee "$out" |
        grep -av "?.*\[no test files\]"
    if [[ ${PIPESTATUS[0]} != 0 ]]; then
        ret=1
    fi

    handleResult "${ret}"
fi


exit ${ret_global}
