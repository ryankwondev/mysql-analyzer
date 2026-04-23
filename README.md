# mysql-analyzer

MySQL 인스턴스 전체를 자동 분석해서 마크다운 리포트로 뽑아주는 쉘 스크립트.

접속 정보만 넣으면 performance_schema, sys, information_schema 등 가용한 기능을 자동 감지하고, 있는 것만 활용해서 분석한다. DML은 일절 실행하지 않음 (SELECT only).

## 분석 항목

- **인스턴스 개요**: DB별 사이즈, 글로벌 상태, 버퍼풀, 서버 변수
- **DB별 쿼리 부하**: 실행 횟수, 총 소요 시간, rows_examined, 풀스캔 비율
- **테이블 분석**: 사이즈 TOP, I/O 핫스팟, 풀스캔 TOP
- **인덱스 분석**: 미사용 인덱스, 중복 인덱스, 인덱스 I/O 통계
- **쿼리 분석**: 슬로우 쿼리, NO_INDEX_USED, 임시 테이블/디스크 정렬 유발 쿼리
- **테이블 구조**: longtext/mediumtext 컬럼, ENUM 컬럼, PK 없는 테이블, 평균 row 사이즈
- **락/대기 분석**: MDL 락, I/O 대기, 파일 I/O, DDL 이력
- **튜닝 권장**: 현재 상태 기반 서버 변수 자동 권장
- **InnoDB 상태**: 데드락, FK 에러, 버퍼풀, row 오퍼레이션
- **비활성 DB 감지**: 쿼리 실행이 없는 DB 목록

## 요구사항

- `mysql` CLI 클라이언트
- MySQL 5.7+ 또는 8.0+ (performance_schema 권장)
- SELECT 권한 (information_schema, performance_schema)

## 사용법

```bash
chmod +x mysql-analyzer.sh

# 전체 인스턴스 분석
./mysql-analyzer.sh -h <host> -P <port> -u <user> -p <password>

# 특정 DB만 분석
./mysql-analyzer.sh -h <host> -P <port> -u <user> -p <password> -d <db_name>

# 개선 항목만 출력 (recommendations only)
./mysql-analyzer.sh -h <host> -P <port> -u <user> -p <password> -r

# 출력 파일 지정
./mysql-analyzer.sh -h <host> -P <port> -u <user> -p <password> -o my-report.md
```

### 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `-h` | 호스트 (필수) | — |
| `-P` | 포트 | 3306 |
| `-u` | 사용자 (필수) | — |
| `-p` | 비밀번호 (필수) | — |
| `-d` | 대상 DB (생략 시 전체 분석) | 전체 |
| `-o` | 출력 파일명 | `mysql-analysis-YYYY-MM-DD.md` |
| `-r` | 개선 항목만 출력 | 전체 분석 |

### 분석 모드

**전체 분석 (기본)**: 인스턴스 개요, DB별 쿼리 부하, 테이블 I/O, 인덱스, 쿼리, 테이블 구조, 락/대기, 튜닝 권장, InnoDB 상태, 비활성 DB — 모든 섹션 출력.

**Recommendations Only (`-r`)**: 조치가 필요한 항목만 출력. 미사용/중복 인덱스, 풀스캔 쿼리, 디스크 정렬 유발 쿼리, 테이블 구조 문제, 서버 변수 튜닝 권장, 비활성 DB. 정보성 섹션은 스킵.

## 기능 자동 감지

스크립트 실행 시 아래 기능의 활성화 여부를 자동으로 체크하고, 가용한 것만 사용한다.

```
✓ performance_schema: ON
✓ sys schema: available
⚠ slow_query_log: OFF
✓ InnoDB: available
✓ Statement digest: has data
✓ Table I/O waits: has data
```

performance_schema가 꺼져 있으면 information_schema 기반으로 축소 분석을 수행한다.

## 출력 예시

```
[00:01:20] Connecting to 10.0.1.100:3306...
  ✓ Connected — MySQL 8.4.5
[00:01:20] Detecting available features...
  ✓ performance_schema: ON
  ✓ sys schema: available
[00:01:20] Starting analysis... (output: full-analysis.md)

[00:01:20] Analyzing instance overview...
  ✓ Instance overview done
[00:01:21] Analyzing per-database query load...
  ✓ Per-database query load done
[00:01:22] Analyzing indexes...
  ✓ Index analysis done
[00:01:35] Analyzing queries...
  ✓ Query analysis done
...
═══════════════════════════════════════════════════════════
  Analysis complete!
  Report saved to: full-analysis.md
═══════════════════════════════════════════════════════════
```

22개 DB 인스턴스 전체 분석 기준 약 30초 소요.

## 주의사항

- DML(INSERT/UPDATE/DELETE)은 실행하지 않음
- performance_schema 통계는 서버 재시작 이후 누적값이므로, 특정 시간대 분석에는 한계가 있음
- 비밀번호가 CLI에 노출되므로 `.my.cnf` 활용 또는 실행 후 히스토리 정리 권장

## License

MIT
