# Clinical Quality Pipeline

두 기관의 합성 진료 검사 데이터를 받아 **Embulk로 수집 → 품질 검사 → DuckDB에 날짜 단위로 원자적 교체 → Great Expectations로 감사**하는 배치 파이프라인입니다.
Airflow와 Digdag 두 오케스트레이터에서 실제로 실행해 같은 결과(스냅샷 해시)를 확인했습니다.

핵심 질문은 하나입니다. **"잘못된 입력이나 중간 장애가 이미 게시된 데이터를 망가뜨리지 않는가?"**

모든 데이터는 자체 생성한 합성 데이터입니다. 실제 환자 데이터·운영 배포·정식 OMOP-CDM 변환은 범위 밖입니다.

## 흐름

```text
기관 A CSV (YYYY-MM-DD, mg/dL) ┐  + hospital_X.rows (송신 측 행 수)
기관 B CSV (YYYY/MM/DD, mmol/L)┘
        │ Embulk 0.11.5 (Docker, 네트워크 차단): 헤더·열 개수 강제, UTF-8/LF 착지
        ▼
run_batch: 헤더 계약 → 행 검증(격리 사유 코드) → 송신 행 수 대조 → 기관별 HMAC 토큰
        → 중복/충돌 판정 → BEGIN; 날짜 삭제·삽입; 참조·건수 검사; COMMIT (실패 시 ROLLBACK)
        ▼
gx_check: 게시된 스냅샷을 GX 1.23 스위트 16항목으로 독립 감사
```

오케스트레이션: `airflow/clinical_daily.py`(Airflow 3.1) · `digdag/clinical.dig`(Digdag 0.10.5)

## 사례

| 문제 | 대응 | 근거 |
|---|---|---|
| 삭제와 삽입 사이에 장애가 나면 그날 데이터가 빈다 | 세 테이블 교체 + 실행 기록을 한 트랜잭션으로. 삭제 직후·삽입 직후 장애 주입 → 이전 해시 유지 → 재시도 성공 | `test_failure_rolls_back_and_retry_commits`, `evidence/demo.json` |
| 같은 행 재전송과 같은 키 다른 값은 다르다 | 완전 중복은 한 번만 저장·건수 기록, 키 충돌은 배치 전체 차단(최신값 추측 안 함) | `test_conflicting_entities_block_whole_batch` |
| **Embulk 첫 실행에서 10,008행 중 884행만 남았다** (오류 없음, 품질 검사 통과) | 병렬 출력 태스크가 같은 파일을 덮어씀 → `min_output_tasks: 1`. 재발 방지로 송신 측 행 수 파일과 대조(`SOURCE_ROW_COUNT_MISMATCH`) | `embulk_compare.py`, `test_truncated_landing_file_blocked_by_control_count` |
| 100만 행에서 Python 행 단위 처리가 병목 | DuckDB 벡터화 엔진: 엄격한 조건을 통과한 행만 SQL로 변환, 나머지는 기존 검증 함수로 → 결과가 같음을 구조로 보장. HMAC도 SQL `sha256`으로 구현 | `test_sql_engine_matches_reference_python_engine`, `evidence/engine-benchmark.json` |

성능(100만 행, 한 PC 측정): 전체 배치 **53.1초 → 22.0초**, 결과 해시 동일.
읽기·검증·토큰 구간은 약 40초 → 2.6초. 남은 시간은 두 엔진 공통인 디스크 커밋·기본키 인덱스·감사 해시입니다(`evidence/engine-breakdown.log`).

## 실행

```sh
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m pytest -q                               # 48 tests
python demo.py --rows 5000                        # 재실행·지연 도착·장애 복구·GX 감사
python benchmark_engines.py --rows 500000         # 엔진 비교 (수 분 소요)
```

Docker가 필요한 검증:

```sh
docker compose run --build --rm tests
python embulk_compare.py --rows 5000              # Embulk 경로 vs 직접 입력
docker build -t clinical-airflow -f airflow/Dockerfile . && docker run --rm --network none --tmpfs /opt/airflow/clinical-data:uid=50000 clinical-airflow bash /opt/airflow/verify.sh
docker build -t clinical-digdag -f digdag/Dockerfile . && docker run --rm --network none --tmpfs /data:uid=10001 clinical-digdag bash verify.sh
```

CI(GitHub Actions, Ubuntu)는 테스트와 위 Docker 검증을 모두 실행합니다.

## 문서

- [설계와 데이터 계약](docs/DESIGN.md): 입력 계약, 품질 판단 표, 트랜잭션, 개인정보 경계
- [의료 도메인 메모](docs/DOMAIN.md): OMOP-CDM 5.4 MEASUREMENT 매핑 격차, 가명정보 관련 조문과 대응
- [검증 기록](docs/VALIDATION.md): 실행한 명령과 실제 결과, 확인하지 않은 것

## 한계

- 합성 데이터·단일 작성자·로컬 파일 기반. 분산 worker, 원격 객체 저장소, 동시 쓰기는 다루지 않습니다.
- 토큰 키는 코드에 공개된 데모 키입니다. **실제 가명처리가 아닙니다.**
- 스키마는 OMOP 관계를 학습한 독자 구조이며 CDM 호환이 아닙니다(격차는 DOMAIN.md).
- Airflow·Digdag는 `dags test` / `digdag run` 로컬 모드로 검증했습니다. 스케줄러 상시 운영은 아닙니다.

## 만든 방식

AI 코딩 도구(Codex, Claude Code)와 함께 작성했습니다. 앞선 프로젝트 [delivery-event-pipeline](https://github.com/RaphaelAhn/delivery-event-pipeline)의
설계 기록에서 찾은 "삭제·삽입 사이 빈 구간" 문제를 이어서 다룬 후속 실험입니다.
