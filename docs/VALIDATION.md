# 검증 기록 — 2026-09-28

모두 합성 데이터. 시간 수치는 한 PC에서의 측정이며 운영 SLA가 아닙니다.

## 환경

- Windows 11, Python 3.12.10, DuckDB 1.4.4, Great Expectations 1.23.2, pytest 8.3.5
- Docker 29.8.0 (Docker Desktop, WSL2). 컨테이너: python:3.12-slim, Airflow 3.1.0, Digdag 0.10.5, Embulk 0.11.5(Temurin 8)
- Linux에서의 실행은 CI(GitHub Actions, Ubuntu) 결과로 확인합니다.

## 실행한 검증

| 명령 | 결과 | 증거 |
|---|---|---|
| `python -m pytest -q` | 48 passed | — |
| `docker compose run --build --rm tests` | 48 passed (컨테이너, 네트워크 차단) | `evidence/docker-tests.log` |
| `python demo.py --rows 5000` | 7회 시도: 성공 5, 의도적 실패 2. 재실행·지연 도착·롤백·재시도 모두 기준 해시와 일치, GX 16/16 | `evidence/demo.json` |
| `python embulk_compare.py --rows 5000` | 직접 입력과 Embulk 경로 스냅샷 해시 동일, 건수·격리 사유 동일 | `evidence/embulk-compare.json` |
| `airflow/verify.sh` (Airflow 3.1.0) | 9/25·9/26 성공, 9/25 재실행 해시 동일, 9/26 잘린 파일 → `SOURCE_ROW_COUNT_MISMATCH`로 실패(재시도 없음), 기존 데이터 유지 | `evidence/airflow-verify.log` |
| `digdag/verify.sh` (Digdag 0.10.5) | 같은 시나리오 + `_error` 알림 기록, 송신 측 재전송 후 성공. Airflow 경로와 날짜별 해시 동일 | `evidence/digdag-verify.log` |
| `python benchmark_engines.py --rows 500000` | 1,000,008행: Python 엔진 53.13초, SQL 엔진 중앙값 21.96초(3회), 해시·건수·격리 목록 동일 | `evidence/engine-benchmark.json` |
| 구간별 측정 | SQL 엔진에서 1초 이상: 커밋 약 5초, 결과 해시 약 5.6초, 기본키 테이블 삽입 약 4초 | `evidence/engine-breakdown.log` |

## 발견하고 고친 것

1. **Embulk 병렬 출력 덮어쓰기**: 첫 비교에서 Embulk 경로가 10,008행 중 884행만 적재. 순번 없는 파일명과 병렬 출력 태스크가 원인.
   `min_output_tasks: 1`로 해결, 송신 측 행 수 대조를 파이프라인에 추가.
2. **Airflow data interval**: 검증 스크립트가 logical date를 처리일로 넘겨 하루 전 폴더를 찾음. logical date D는 [D-1, D)를 처리하도록 스크립트 수정.
3. **Digdag 이미지의 Python 3.14**: Temurin 기본 이미지의 Python에 duckdb·GX wheel이 없어 python:3.12-slim에 JRE를 복사하는 방식으로 변경.

## 확인하지 않은 것

- Airflow 스케줄러 상시 실행·백필 명령, Digdag 서버 모드
- 100만 행을 넘는 규모, 원격 저장소, 동시 쓰기
- 실제 의료 데이터, 법적 가명처리 적정성, 정식 OMOP 변환
