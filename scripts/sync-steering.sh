#!/usr/bin/env bash
#
# harness-playbook 서브모듈에서 steering 지침을 동기화한다.
#
# 동작:
#   1. harness-playbook 서브모듈을 원격 최신 커밋으로 업데이트
#   2. 서브모듈의 모든 steering 파일을 .kiro/steering/에 상대경로 심링크로 재생성
#      (업스트림에서 추가/삭제된 파일 반영)
#   3. 업스트림에 없는 오래된 심링크 정리
#
# 사용법: ./scripts/sync-steering.sh
set -euo pipefail

# 리포 루트 (이 스크립트의 부모 디렉토리)
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUBMODULE_STEERING="$REPO_ROOT/vendor/harness-playbook/.kiro/steering"
LOCAL_STEERING="$REPO_ROOT/.kiro/steering"

echo "==> harness-playbook 서브모듈을 원격 최신 커밋으로 업데이트..."
git -C "$REPO_ROOT" submodule update --remote --init vendor/harness-playbook

if [ ! -d "$SUBMODULE_STEERING" ]; then
  echo "ERROR: $SUBMODULE_STEERING 를 찾을 수 없습니다. 업스트림 구조가 바뀌었나요?" >&2
  exit 1
fi

mkdir -p "$LOCAL_STEERING"

echo "==> 오래된 steering 심링크 정리..."
# 심링크만 제거한다 (로컬 실제 파일은 건드리지 않음).
find "$LOCAL_STEERING" -maxdepth 1 -type l -name '*.md' | while read -r link; do
  name="$(basename "$link")"
  if [ ! -f "$SUBMODULE_STEERING/$name" ]; then
    echo "    - $name 제거 (업스트림에서 삭제됨)"
    rm -f "$link"
  fi
done

echo "==> 서브모듈 steering 파일 링크..."
for f in "$SUBMODULE_STEERING"/*.md; do
  name="$(basename "$f")"
  ln -sf "../../vendor/harness-playbook/.kiro/steering/$name" "$LOCAL_STEERING/$name"
  echo "    + $name"
done

echo "==> 완료. 현재 steering 파일:"
ls -l "$LOCAL_STEERING"
