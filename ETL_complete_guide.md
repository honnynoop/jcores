# ETL (Extract · Transform · Load) 완전 가이드

> **ETL**은 다양한 소스에서 데이터를 추출(Extract)하고, 분석 목적에 맞게 변환(Transform)한 뒤, 목적지 시스템에 적재(Load)하는 데이터 파이프라인의 핵심 프로세스다.

---

## 목차

1. [ETL 개요](#1-etl-개요)
2. [E — 추출 (Extract)](#2-e--추출-extract)
3. [T — 변환 (Transform)](#3-t--변환-transform)
   - 3.1 데이터 정제 (Cleansing)
   - 3.2 결측값 처리 (Missing Value Handling)
   - 3.3 이상값 처리 (Outlier Handling)
   - 3.4 형식 변환 (Type Conversion)
   - 3.5 문자열 변환 (String Transformation)
   - 3.6 스케일링 (Scaling / Normalization)
   - 3.7 인코딩 (Encoding)
   - 3.8 집계 (Aggregation)
   - 3.9 조인 (Join / Merge)
   - 3.10 파생 변수 생성 (Feature Engineering)
   - 3.11 피벗 / 언피벗 (Pivot / Unpivot)
   - 3.12 중복 제거 (Deduplication)
   - 3.13 날짜·시간 변환 (Datetime Transformation)
   - 3.14 데이터 검증 (Validation)
4. [L — 적재 (Load)](#4-l--적재-load)
5. [전체 ETL 파이프라인 예제](#5-전체-etl-파이프라인-예제)
6. [ETL vs ELT](#6-etl-vs-elt)

---

## 1. ETL 개요

```
┌──────────────┐     ┌──────────────────────┐     ┌──────────────┐
│   Source      │     │     Transform         │     │  Destination │
│ ─────────── │     │ ─────────────────── │     │ ─────────── │
│  DB (RDB)    │     │  정제  →  변환  →    │     │  DW / DM    │
│  CSV / Excel │ ──▶ │  집계  →  검증  →    │ ──▶ │  Data Lake  │
│  API / JSON  │     │  인코딩→  파생변수   │     │  BI / ML    │
│  Log / File  │     │                       │     │             │
└──────────────┘     └──────────────────────┘     └──────────────┘
      Extract                Transform                   Load
```

### 왜 ETL이 필요한가?

- 소스 데이터는 형식·품질·구조가 제각각이다
- 분석·ML 모델은 깨끗하고 일관된 데이터를 전제로 한다
- ETL은 **Raw Data → Trusted Data**로의 신뢰성 있는 변환 과정이다

### 공통 라이브러리

```python
import pandas as pd
import numpy as np
from sklearn.preprocessing import (
    MinMaxScaler, StandardScaler, LabelEncoder, OneHotEncoder
)
from sqlalchemy import create_engine
import re
import warnings
warnings.filterwarnings("ignore")

# 샘플 데이터 생성
np.random.seed(42)
raw_data = {
    "id":        [1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10],
    "name":      ["Alice", "bob", "  Charlie  ", "DAVE", None, "Eve", "frank", "Grace", "HEIDI", "ivan", "judy"],
    "age":       [25, 17, None, 200, 30, 30, 22, None, 45, 33, 28],
    "salary":    [50000, 32000, 75000, None, 62000, 62000, 41000, 88000, 95000, 54000, 71000],
    "gender":    ["F", "M", "M", "m", "F", "F", "M", None, "F", "M", "F"],
    "join_date": ["2020-01-15", "2021-03-22", "2019/07/08", "20220510", None,
                  "2022-05-30", "2023-01-01", "2020-11-30", "2018-06-15", "2021-08-20", "2022-03-10"],
    "dept":      ["HR", "IT", "Finance", "IT", "HR", "HR", "Finance", "IT", "Finance", "HR", "IT"],
    "score":     [88, 72, 95, 60, 85, 85, 78, 91, 55, 82, 90],
}
df = pd.DataFrame(raw_data)
print(df)
```

---

## 2. E — 추출 (Extract)

다양한 소스에서 원천 데이터를 읽어오는 단계.

```python
# ── CSV / Excel
df_csv   = pd.read_csv("data.csv", encoding="utf-8")
df_excel = pd.read_excel("data.xlsx", sheet_name="Sheet1")

# ── RDBMS (SQLAlchemy)
engine = create_engine("postgresql://user:pw@host:5432/mydb")
df_db  = pd.read_sql("SELECT * FROM employees WHERE dept = 'IT'", engine)

# ── REST API
import requests
resp = requests.get("https://api.example.com/users", headers={"Authorization": "Bearer TOKEN"})
df_api = pd.DataFrame(resp.json()["data"])

# ── JSON / NDJSON
df_json  = pd.read_json("data.json")
df_ndjson = pd.read_json("data.ndjson", lines=True)

# ── 로그 파일 (정규식 파싱)
import re
log_pattern = r'(?P<ip>\S+) .+ \[(?P<time>.+?)\] "(?P<method>\w+) (?P<path>\S+)'
with open("access.log") as f:
    rows = [re.match(log_pattern, line).groupdict() for line in f if re.match(log_pattern, line)]
df_log = pd.DataFrame(rows)
```

---

## 3. T — 변환 (Transform)

ETL에서 가장 복잡하고 중요한 단계. 14가지 세부 유형으로 나눠 설명한다.

---

### 3.1 데이터 정제 (Cleansing)

불필요한 공백 제거, 대소문자 통일, 특수문자 제거 등 기본적인 정제 작업.

```python
df_clean = df.copy()

# 문자열 컬럼 공백 제거 + 대소문자 통일
df_clean["name"] = df_clean["name"].str.strip().str.title()

# gender 대문자 통일
df_clean["gender"] = df_clean["gender"].str.upper().str.strip()

# 이름에서 숫자·특수문자 제거
df_clean["name"] = df_clean["name"].str.replace(r"[^A-Za-z\s]", "", regex=True)

print(df_clean[["name", "gender"]].head(8))
# 결과:
#       name gender
# 0    Alice      F
# 1      Bob      M
# 2  Charlie      M
# 3     Dave      M
# 4     None      F
```

---

### 3.2 결측값 처리 (Missing Value Handling)

결측값 탐지 → 처리 전략 선택 → 적용.

```python
# ── 결측값 현황 파악
print("결측값 개수:\n", df_clean.isnull().sum())
print("\n결측값 비율:\n", (df_clean.isnull().sum() / len(df_clean) * 100).round(2))

# ── 전략 1: 제거 (결측 비율이 낮을 때)
df_drop = df_clean.dropna(subset=["name"])          # 특정 컬럼 기준
df_drop = df_clean.dropna(thresh=int(len(df_clean.columns) * 0.7))  # 70% 이상 있는 행만 유지

# ── 전략 2: 수치형 → 통계값으로 대체
df_clean["age"]    = df_clean["age"].fillna(df_clean["age"].median())
df_clean["salary"] = df_clean["salary"].fillna(df_clean["salary"].mean())

# ── 전략 3: 범주형 → 최빈값(mode) 대체
df_clean["gender"] = df_clean["gender"].fillna(df_clean["gender"].mode()[0])

# ── 전략 4: 시계열 → 앞/뒤 값으로 채우기 (forward/backward fill)
df_ts = df_clean.sort_values("join_date").copy()
df_ts["salary"] = df_ts["salary"].ffill().bfill()

# ── 전략 5: 보간 (interpolation)
df_clean["salary"] = df_clean["salary"].interpolate(method="linear")

# ── 전략 6: 그룹별 평균 대체 (dept별 salary 평균으로)
df_clean["salary"] = df_clean.groupby("dept")["salary"].transform(
    lambda x: x.fillna(x.mean())
)

print("\n처리 후 결측값:\n", df_clean.isnull().sum())
```

---

### 3.3 이상값 처리 (Outlier Handling)

데이터 분포를 크게 벗어나는 값을 탐지하고 처리.

```python
# ── 방법 1: IQR (Interquartile Range) 기반
def cap_outliers_iqr(series, multiplier=1.5):
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - multiplier * IQR
    upper = Q3 + multiplier * IQR
    return series.clip(lower=lower, upper=upper), lower, upper

df_clean["salary_capped"], lo, hi = cap_outliers_iqr(df_clean["salary"])
print(f"salary 이상값 범위: {lo:.0f} ~ {hi:.0f}")

# ── 방법 2: Z-Score (평균±3σ 밖을 이상값으로)
from scipy import stats

z_scores = np.abs(stats.zscore(df_clean["age"].dropna()))
outlier_mask = z_scores > 3
print(f"Z-Score 이상값 개수: {outlier_mask.sum()}")

# ── age 도메인 규칙 기반 이상값 처리 (18세 이상 65세 이하 근로자)
df_clean["age"] = df_clean["age"].clip(lower=18, upper=65)

# ── 방법 3: 이상값 행 제거
df_no_outlier = df_clean[df_clean["age"].between(18, 65)]
```

---

### 3.4 형식 변환 (Type Conversion)

데이터 타입을 목적지 시스템에 맞게 변환.

```python
# ── 숫자형 변환 (강제 변환 시 errors='coerce'로 NaN 처리)
df_clean["age"]    = pd.to_numeric(df_clean["age"], errors="coerce").astype("Int64")
df_clean["salary"] = pd.to_numeric(df_clean["salary"], errors="coerce").astype(float)
df_clean["score"]  = df_clean["score"].astype(int)

# ── 불리언 변환
df_clean["is_senior"] = (df_clean["age"] >= 40).astype(bool)

# ── Categorical 변환 (메모리 절약, 그룹 연산 최적화)
df_clean["dept"]   = df_clean["dept"].astype("category")
df_clean["gender"] = df_clean["gender"].astype("category")

print(df_clean.dtypes)
# id           int64
# age          Int64
# salary      float64
# dept       category
# gender     category
# is_senior     bool
```

---

### 3.5 문자열 변환 (String Transformation)

문자열 패턴 추출, 분리, 치환 등.

```python
# ── 전화번호 정규화 (하이픈 제거 후 형식 통일)
phones = pd.Series(["010-1234-5678", "01056789012", "010.9876.5432"])
phones_clean = phones.str.replace(r"[^0-9]", "", regex=True)
phones_norm  = phones_clean.str.replace(r"(\d{3})(\d{4})(\d{4})", r"\1-\2-\3", regex=True)
print(phones_norm.tolist())
# ['010-1234-5678', '010-5678-9012', '010-9876-5432']

# ── 이메일에서 도메인 추출
emails = pd.Series(["alice@gmail.com", "bob@company.co.kr", "charlie@naver.com"])
df_email = pd.DataFrame({"email": emails})
df_email["domain"] = df_email["email"].str.extract(r"@(.+)$")
df_email["tld"]    = df_email["email"].str.extract(r"\.(\w+)$")
print(df_email)

# ── 주소에서 시/도 추출
addresses = pd.Series(["서울특별시 강남구 테헤란로", "경기도 성남시 분당구", "부산광역시 해운대구"])
sido = addresses.str.extract(r"^(서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충청[남북]?|전[라라]?[남북]?|경[상상]?[남북]?|제주)")
print(sido)

# ── 문자열 분리 (성과 이름 분리)
full_names = pd.Series(["Kim Minjun", "Lee Soyeon", "Park Jiwoo"])
df_names = full_names.str.split(" ", expand=True).rename(columns={0: "last", 1: "first"})
print(df_names)
```

---

### 3.6 스케일링 (Scaling / Normalization)

수치형 피처의 범위를 통일해 모델 학습 성능을 개선.

```python
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler

numeric_cols = ["age", "salary", "score"]
df_scale = df_clean[numeric_cols].dropna().copy()

# ── Min-Max 정규화 (범위: 0~1)
# 언제: 신경망, KNN, SVM에 유용. 이상값에 민감.
minmax = MinMaxScaler()
df_scale["salary_minmax"] = minmax.fit_transform(df_scale[["salary"]])

# ── Z-Score 표준화 (평균 0, 표준편차 1)
# 언제: 선형 회귀, 로지스틱 회귀. 정규분포 가정.
stdscaler = StandardScaler()
df_scale["salary_zscore"] = stdscaler.fit_transform(df_scale[["salary"]])

# ── Robust 스케일링 (IQR 기반, 이상값에 강인)
# 언제: 이상값이 많은 실무 데이터에 추천.
robustscaler = RobustScaler()
df_scale["salary_robust"] = robustscaler.fit_transform(df_scale[["salary"]])

# ── Log 변환 (심한 우편향 분포 완화)
df_scale["salary_log"] = np.log1p(df_scale["salary"])

print(df_scale[["salary", "salary_minmax", "salary_zscore", "salary_robust", "salary_log"]].round(4))
```

| 방식 | 수식 | 특징 | 적합 상황 |
|------|------|------|-----------|
| Min-Max | (x - min) / (max - min) | 0~1 범위 | 신경망, KNN |
| Z-Score | (x - μ) / σ | 평균 0, 표준편차 1 | 선형 모델 |
| Robust | (x - Q2) / IQR | 이상값에 강인 | 이상값 많은 데이터 |
| Log | log(1 + x) | 우편향 완화 | 매출, 인구 등 |

---

### 3.7 인코딩 (Encoding)

범주형 변수를 수치로 변환해 ML 모델에서 사용 가능하게 만듦.

```python
df_enc = df_clean[["gender", "dept", "score"]].dropna().copy()

# ── Label Encoding (순서 있는 범주에 적합: 낮음/중간/높음)
le = LabelEncoder()
df_enc["gender_label"] = le.fit_transform(df_enc["gender"])
print("Label Encoding 클래스:", le.classes_)   # ['F' 'M']

# ── One-Hot Encoding (순서 없는 범주에 적합)
# drop_first=True: 다중공선성 방지를 위해 첫 번째 더미 제거
df_enc = pd.get_dummies(df_enc, columns=["dept"], drop_first=False, dtype=int)
print(df_enc.filter(like="dept").head(4))
# dept_Finance  dept_HR  dept_IT
#            1        0        0
#            0        1        0
#            0        0        1

# ── Ordinal Encoding (점수를 등급으로 변환 후 수치화)
def score_to_grade(s):
    if   s >= 90: return 4  # A
    elif s >= 80: return 3  # B
    elif s >= 70: return 2  # C
    elif s >= 60: return 1  # D
    else:         return 0  # F

df_enc["grade_ord"] = df_enc["score"].apply(score_to_grade)

# ── Target Encoding (범주별 타겟 평균으로 대체, 고카디널리티에 유용)
target_mean = df_clean.groupby("dept")["salary"].mean()
df_clean["dept_target_enc"] = df_clean["dept"].map(target_mean)
print(target_mean)
```

---

### 3.8 집계 (Aggregation)

여러 행을 그룹화해 통계 지표를 계산.

```python
# ── 기본 집계
agg_dept = df_clean.groupby("dept").agg(
    headcount    = ("id",     "count"),
    avg_salary   = ("salary", "mean"),
    max_salary   = ("salary", "max"),
    min_salary   = ("salary", "min"),
    salary_std   = ("salary", "std"),
    avg_score    = ("score",  "mean"),
).round(1).reset_index()
print(agg_dept)

# ── 다중 컬럼 동시 집계
agg_multi = df_clean.groupby(["dept", "gender"]).agg(
    count     = ("id",     "count"),
    avg_sal   = ("salary", "mean"),
    avg_score = ("score",  "mean"),
).reset_index()
print(agg_multi)

# ── 누적·이동 집계 (시계열에서 활용)
df_ts = df_clean.sort_values("join_date").copy()
df_ts["cum_headcount"]   = df_ts.groupby("dept").cumcount() + 1
df_ts["rolling_avg_sal"] = (
    df_ts.groupby("dept")["salary"]
         .transform(lambda x: x.rolling(3, min_periods=1).mean())
)

# ── 윈도우 함수: 부서 내 급여 순위
df_clean["salary_rank_in_dept"] = (
    df_clean.groupby("dept")["salary"]
            .rank(method="dense", ascending=False)
            .astype(int)
)
print(df_clean[["name", "dept", "salary", "salary_rank_in_dept"]].sort_values("dept"))
```

---

### 3.9 조인 (Join / Merge)

두 개 이상의 데이터프레임을 키 기준으로 결합.

```python
# 샘플 추가 테이블
dept_info = pd.DataFrame({
    "dept":     ["HR", "IT", "Finance"],
    "dept_head": ["Kim", "Lee", "Park"],
    "budget":   [500_000, 1_200_000, 900_000],
})

bonus_info = pd.DataFrame({
    "id":    [1, 3, 5, 8, 10],
    "bonus": [3000, 5000, 4000, 6000, 3500],
})

# ── Inner Join (교집합)
df_inner = df_clean.merge(dept_info, on="dept", how="inner")

# ── Left Join (왼쪽 기준, 오른쪽 없으면 NaN)
df_left = df_clean.merge(bonus_info, on="id", how="left")
df_left["bonus"] = df_left["bonus"].fillna(0)

# ── 다중 키 조인
df_multi_key = df_clean.merge(agg_dept, on="dept", how="left")

# ── Self Join (같은 부서 직원 페어링)
df_self = df_clean[["id", "name", "dept"]].merge(
    df_clean[["id", "name", "dept"]],
    on="dept",
    suffixes=("_a", "_b")
)
df_self = df_self[df_self["id_a"] < df_self["id_b"]]  # 중복 제거

print("Left Join 후 컬럼:", df_left.columns.tolist())
print(df_left[["name", "dept", "salary", "bonus"]].head())
```

---

### 3.10 파생 변수 생성 (Feature Engineering)

기존 컬럼으로 새로운 의미 있는 변수를 만들어 분석·모델링에 활용.

```python
df_feat = df_left.copy()

# ── 수치 연산 파생 변수
df_feat["total_comp"]       = df_feat["salary"] + df_feat["bonus"]
df_feat["salary_per_score"] = (df_feat["salary"] / df_feat["score"]).round(1)

# ── 비율 파생 (부서 평균 대비 급여 비율)
dept_avg = df_feat.groupby("dept")["salary"].transform("mean")
df_feat["salary_vs_dept_avg"] = (df_feat["salary"] / dept_avg).round(3)

# ── 범주형 파생 (급여 구간)
df_feat["salary_band"] = pd.cut(
    df_feat["salary"],
    bins  = [0, 40_000, 60_000, 80_000, np.inf],
    labels= ["하", "중", "상", "최상"]
)

# ── 점수 등급
df_feat["grade"] = pd.cut(
    df_feat["score"],
    bins  = [0, 60, 70, 80, 90, 100],
    labels= ["F", "D", "C", "B", "A"],
    right = True
)

# ── Boolean 파생
df_feat["high_performer"] = (df_feat["score"] >= 85) & (df_feat["salary"] >= 60_000)
df_feat["senior"]         = df_feat["age"] >= 40

# ── 상호작용 변수 (Interaction Feature)
df_feat["age_x_score"] = df_feat["age"] * df_feat["score"]

print(df_feat[["name", "total_comp", "salary_band", "grade", "high_performer"]].head(8))
```

---

### 3.11 피벗 / 언피벗 (Pivot / Unpivot)

데이터의 행·열 구조를 전환해 분석 목적에 맞는 형태로 재구성.

```python
# ── Pivot: 부서 x 성별 별 평균 급여 매트릭스
pivot_table = df_clean.pivot_table(
    values   = "salary",
    index    = "dept",
    columns  = "gender",
    aggfunc  = "mean",
    fill_value = 0,
    margins  = True,       # 행/열 합계 추가
    margins_name = "전체"
).round(0)
print("Pivot Table:\n", pivot_table)

# ── 다중 집계 피벗
pivot_multi = df_clean.pivot_table(
    values  = ["salary", "score"],
    index   = "dept",
    columns = "gender",
    aggfunc = {"salary": "mean", "score": ["mean", "max"]},
).round(1)
print("\n다중 집계 피벗:\n", pivot_multi)

# ── Unpivot (Wide → Long): melt
df_wide = pd.DataFrame({
    "dept":    ["HR", "IT", "Finance"],
    "Q1_sale": [100, 200, 150],
    "Q2_sale": [120, 210, 160],
    "Q3_sale": [130, 190, 170],
    "Q4_sale": [140, 220, 180],
})

df_long = df_wide.melt(
    id_vars    = "dept",
    value_vars = ["Q1_sale", "Q2_sale", "Q3_sale", "Q4_sale"],
    var_name   = "quarter",
    value_name = "sale"
)
df_long["quarter"] = df_long["quarter"].str.replace("_sale", "")
print("\nLong Format:\n", df_long.head(8))
```

---

### 3.12 중복 제거 (Deduplication)

완전 중복, 부분 중복, 근사 중복을 식별하고 처리.

```python
df_dedup = df_clean.copy()

# ── 완전 중복 확인 및 제거
n_duplicates = df_dedup.duplicated().sum()
print(f"완전 중복 행: {n_duplicates}개")
df_dedup = df_dedup.drop_duplicates()

# ── 키 기준 중복 (id 기준 최신 레코드 유지)
df_dedup = df_dedup.sort_values("join_date", ascending=False)
df_dedup = df_dedup.drop_duplicates(subset=["id"], keep="first")

# ── 비즈니스 키 기준 중복 (이름+부서 조합 중복 제거)
df_dedup = df_dedup.drop_duplicates(subset=["name", "dept"], keep="last")

# ── 근사 중복 (Fuzzy Deduplication): 이름 유사도 기반
# pip install rapidfuzz
from rapidfuzz import fuzz, process

def find_similar_names(names, threshold=85):
    seen = []
    duplicates = []
    for name in names:
        match = process.extractOne(name, seen, scorer=fuzz.ratio)
        if match and match[1] >= threshold:
            duplicates.append((name, match[0], match[1]))
        else:
            seen.append(name)
    return duplicates

similar = find_similar_names(df_dedup["name"].dropna().tolist())
print("근사 중복 후보:", similar)

print(f"\n중복 제거 후 행 수: {len(df_dedup)}")
```

---

### 3.13 날짜·시간 변환 (Datetime Transformation)

다양한 형식의 날짜 문자열을 파싱하고 유용한 시간 피처를 추출.

```python
df_dt = df_clean.copy()

# ── 다양한 날짜 형식 일괄 파싱
def parse_date_flexible(val):
    """여러 포맷 자동 탐지"""
    if pd.isna(val): return pd.NaT
    val = str(val).strip()
    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%d-%m-%Y", "%m/%d/%Y"]:
        try:
            return pd.to_datetime(val, format=fmt)
        except:
            continue
    return pd.to_datetime(val, infer_datetime_format=True, errors="coerce")

df_dt["join_dt"] = df_dt["join_date"].apply(parse_date_flexible)

# ── 시간 피처 추출
ref_date = pd.Timestamp("2024-01-01")
df_dt["join_year"]    = df_dt["join_dt"].dt.year
df_dt["join_month"]   = df_dt["join_dt"].dt.month
df_dt["join_quarter"] = df_dt["join_dt"].dt.quarter
df_dt["join_weekday"] = df_dt["join_dt"].dt.day_name()
df_dt["tenure_days"]  = (ref_date - df_dt["join_dt"]).dt.days
df_dt["tenure_years"] = (df_dt["tenure_days"] / 365.25).round(1)
df_dt["is_weekend_join"] = df_dt["join_dt"].dt.dayofweek >= 5

# ── 시간대 변환 (UTC → KST)
df_dt["join_utc"] = df_dt["join_dt"].dt.tz_localize("UTC")
df_dt["join_kst"] = df_dt["join_utc"].dt.tz_convert("Asia/Seoul")

print(df_dt[["name", "join_date", "join_dt", "tenure_years", "join_quarter"]].head(8))
```

---

### 3.14 데이터 검증 (Validation)

변환 후 데이터가 비즈니스 규칙과 품질 기준을 만족하는지 확인.

```python
def validate_dataframe(df: pd.DataFrame) -> dict:
    """데이터 품질 검증 리포트 생성"""
    report = {}

    # 1. 결측값 비율
    missing_rate = (df.isnull().sum() / len(df) * 100).round(2)
    report["missing_rate"] = missing_rate[missing_rate > 0].to_dict()

    # 2. 도메인 규칙 검증
    violations = []

    if "age" in df.columns:
        bad_age = df[~df["age"].between(18, 65) & df["age"].notna()]
        if len(bad_age): violations.append(f"age 범위 위반: {len(bad_age)}건")

    if "salary" in df.columns:
        bad_sal = df[df["salary"] < 0]
        if len(bad_sal): violations.append(f"salary 음수: {len(bad_sal)}건")

    if "gender" in df.columns:
        bad_gen = df[~df["gender"].isin(["M", "F", None]) & df["gender"].notna()]
        if len(bad_gen): violations.append(f"gender 허용 외 값: {len(bad_gen)}건")

    if "score" in df.columns:
        bad_score = df[~df["score"].between(0, 100)]
        if len(bad_score): violations.append(f"score 범위 위반: {len(bad_score)}건")

    report["violations"] = violations

    # 3. 고유값 검증
    if "id" in df.columns:
        dup_ids = df["id"].duplicated().sum()
        report["duplicate_ids"] = int(dup_ids)

    # 4. 데이터 분포 요약
    report["row_count"]    = len(df)
    report["column_count"] = len(df.columns)
    report["dtypes"]       = df.dtypes.astype(str).to_dict()

    return report

report = validate_dataframe(df_clean)
import json
print(json.dumps(report, ensure_ascii=False, indent=2))
```

---

## 4. L — 적재 (Load)

검증된 데이터를 목적지 시스템에 저장.

```python
df_final = df_feat.copy()

# ── CSV / Parquet 저장
df_final.to_csv("output/employees_clean.csv", index=False, encoding="utf-8-sig")
df_final.to_parquet("output/employees_clean.parquet", index=False)

# ── RDBMS (SQLAlchemy)
engine = create_engine("postgresql://user:pw@host:5432/mydb")

# 전체 교체 (replace)
df_final.to_sql("employees_clean", engine, if_exists="replace", index=False)

# 증분 적재 (append)
df_new = df_final[df_final["join_date"] >= "2023-01-01"]
df_new.to_sql("employees_clean", engine, if_exists="append", index=False)

# ── Upsert (Insert or Update) — PostgreSQL 예시
from sqlalchemy import text

def upsert_postgres(df, table, key_col, engine):
    """키 기준 충돌 시 업데이트"""
    cols = ", ".join(df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in df.columns if c != key_col])
    sql = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({key_col}) DO UPDATE SET {updates}
    """
    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict("records"))

# ── 파티션 저장 (Hive-style, 연·월 파티셔닝)
df_final["year"]  = pd.to_datetime(df_final["join_date"]).dt.year
df_final["month"] = pd.to_datetime(df_final["join_date"]).dt.month
df_final.to_parquet(
    "output/partitioned/",
    partition_cols=["year", "month"],
    index=False
)
```

---

## 5. 전체 ETL 파이프라인 예제

클래스 기반으로 재사용 가능한 ETL 파이프라인을 구성.

```python
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Callable, Optional
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


@dataclass
class ETLConfig:
    source_path:      str
    dest_path:        str
    numeric_cols:     List[str] = field(default_factory=list)
    categorical_cols: List[str] = field(default_factory=list)
    date_cols:        List[str] = field(default_factory=list)
    id_col:           str = "id"
    drop_threshold:   float = 0.5   # 결측 50% 초과 컬럼 제거


class ETLPipeline:
    def __init__(self, config: ETLConfig):
        self.config = config
        self.df: Optional[pd.DataFrame] = None
        self._steps: List[Callable] = []

    def register(self, func: Callable):
        """변환 스텝 등록 데코레이터"""
        self._steps.append(func)
        return func

    # ── Extract
    def extract(self) -> "ETLPipeline":
        log.info(f"[Extract] {self.config.source_path}")
        self.df = pd.read_csv(self.config.source_path)
        log.info(f"  → {self.df.shape[0]}행 {self.df.shape[1]}열 로드 완료")
        return self

    # ── Transform (등록된 스텝 순서대로 실행)
    def transform(self) -> "ETLPipeline":
        log.info("[Transform] 변환 시작")
        for step in self._steps:
            step_name = step.__name__
            before = self.df.shape
            self.df = step(self.df, self.config)
            after  = self.df.shape
            log.info(f"  [{step_name}] {before} → {after}")
        return self

    # ── Load
    def load(self) -> "ETLPipeline":
        log.info(f"[Load] {self.config.dest_path}")
        self.df.to_parquet(self.config.dest_path, index=False)
        log.info(f"  → {len(self.df)}행 저장 완료")
        return self

    def run(self):
        return self.extract().transform().load()


# ── 변환 스텝 정의
def step_drop_high_missing(df, cfg):
    thresh = int(len(df) * (1 - cfg.drop_threshold))
    return df.dropna(thresh=thresh, axis=1)

def step_clean_strings(df, cfg):
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.title()
    return df

def step_fill_missing(df, cfg):
    for col in cfg.numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())
    for col in cfg.categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mode()[0])
    return df

def step_parse_dates(df, cfg):
    for col in cfg.date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def step_remove_duplicates(df, cfg):
    return df.drop_duplicates(subset=[cfg.id_col], keep="last")

def step_add_features(df, cfg):
    if "salary" in df.columns and "score" in df.columns:
        df["salary_per_score"] = (df["salary"] / df["score"]).round(2)
    if "age" in df.columns:
        df["is_senior"] = df["age"] >= 40
    return df

def step_validate(df, cfg):
    if "age" in df.columns:
        df = df[df["age"].between(18, 65) | df["age"].isna()]
    if "score" in df.columns:
        df = df[df["score"].between(0, 100) | df["score"].isna()]
    return df


# ── 파이프라인 실행
config = ETLConfig(
    source_path      = "raw_employees.csv",
    dest_path        = "clean_employees.parquet",
    numeric_cols     = ["age", "salary", "score"],
    categorical_cols = ["gender", "dept"],
    date_cols        = ["join_date"],
    id_col           = "id",
)

pipeline = ETLPipeline(config)

# 스텝 등록 순서 = 실행 순서
pipeline._steps = [
    step_drop_high_missing,
    step_clean_strings,
    step_fill_missing,
    step_parse_dates,
    step_remove_duplicates,
    step_add_features,
    step_validate,
]

# pipeline.run()   ← 실제 파일이 있을 때 실행
```

---

## 6. ETL vs ELT

| 구분 | ETL | ELT |
|------|-----|-----|
| **순서** | Extract → Transform → Load | Extract → Load → Transform |
| **변환 위치** | 중간 스테이징 서버 | 목적지 DW·Data Lake 내부 |
| **적합한 저장소** | 전통 DW (Oracle, Teradata) | 클라우드 DW (BigQuery, Snowflake, Redshift) |
| **데이터 규모** | 중소규모 | 대규모 (수 TB ~ PB) |
| **유연성** | 변환 먼저 → 재처리 비용 큼 | Raw 보존 → 필요 시 재변환 |
| **대표 도구** | Apache Airflow, Talend, SSIS | dbt, Spark, BigQuery |

---

## 참고 자료

- Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit*, 3rd ed.
- pandas 공식 문서: https://pandas.pydata.org/docs/
- scikit-learn 전처리: https://scikit-learn.org/stable/modules/preprocessing.html
