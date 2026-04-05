# pandas `pivot_table` & `melt` 정리

> **핵심 관계**: 두 함수는 서로 역방향 — `pivot_table`은 long → wide, `melt`는 wide → long

---

## 변환 방향 개요

```
Long format                         Wide format
────────────────────               ────────────────────────────
country   year   pop               country   1952    1957   ...
Albania   1952   1282697    ──▶    Albania   1282697 1476505
Albania   1957   1476505   pivot   Zimbabwe  3080907 3308800
Zimbabwe  1952   3080907   ◀──
Zimbabwe  1957   3308800   melt
...
```

| 방향 | 함수 | 결과 | 용도 |
|---|---|---|---|
| long → **wide** | `pivot_table` | 열이 늘어남 | 리포트·비교·크로스탭 |
| wide → **long** | `melt` | 행이 늘어남 | 분석·시각화·모델링 |

---

## 1. `pivot_table` — long → wide

### 함수 시그니처

```python
DataFrame.pivot_table(
    index=None,      # 행(row)이 될 컬럼
    columns=None,    # 열(column 헤더)이 될 컬럼
    values=None,     # 채울 값
    aggfunc='mean',  # 중복 처리 집계 함수
    fill_value=None  # NaN 대체값 (선택)
)
```

### 주요 파라미터

| 파라미터 | 설명 | 예시 |
|---|---|---|
| `index` | 행 식별자 | `'country'` |
| `columns` | 새 열 헤더 | `'year'` |
| `values` | 셀에 채울 값 | `'pop'` |
| `aggfunc` | 중복 집계 방법 | `'sum'`, `'mean'`, `'count'`, `'first'` |
| `fill_value` | 결측값 대체 | `0` |

### 예제 코드

```python
import pandas as pd

# 원본: long format (ss)
# country / year / pop 컬럼

# ✅ pivot_table: long → wide
wide = ss.pivot_table(
    index='country',    # → 행
    columns='year',     # → 열 헤더 (1952, 1957, ...)
    values='pop',       # → 셀 값
    aggfunc='sum'       # 중복 시 합산
)

# 컬럼명 정리
wide.columns.name = None        # 'year' 라벨 제거
wide = wide.reset_index()       # country를 인덱스 → 일반 컬럼으로

print(wide.head(2))
# country     1952     1957  ...
# Albania  1282697  1476505  ...
# Zimbabwe 3080907  3308800  ...
```

### ⚠️ `pivot` vs `pivot_table` 차이

| | `pivot` | `pivot_table` |
|---|---|---|
| 중복 `(index, columns)` | **에러 발생** | `aggfunc`으로 처리 |
| 결측값 처리 | 없음 | `fill_value` 지원 |
| 권장 상황 | 중복 없음이 확실할 때 | 실무·시험 — **항상 안전** ✅ |

---

## 2. `melt` — wide → long

### 함수 시그니처

```python
DataFrame.melt(
    id_vars=None,       # 그대로 유지할 컬럼 (식별자)
    value_vars=None,    # 녹일 컬럼들 (생략 시 id_vars 외 전부)
    var_name='variable',  # 컬럼명을 담을 새 컬럼 이름
    value_name='value'    # 값을 담을 새 컬럼 이름
)
```

### 주요 파라미터

| 파라미터 | 설명 | 예시 |
|---|---|---|
| `id_vars` | 유지할 컬럼 (식별자) | `'country'` |
| `value_vars` | 녹일 컬럼 목록 | `[1952, 1957, ...]` |
| `var_name` | 녹인 컬럼명 저장 | `'year'` |
| `value_name` | 값 저장 컬럼명 | `'pop'` |

### 예제 코드

```python
# ✅ melt: wide → long
long = wide.melt(
    id_vars='country',      # 유지
    var_name='year',        # 1952, 1957... 이 들어갈 컬럼명
    value_name='pop'        # 인구수가 들어갈 컬럼명
)

print(long.head(4))
#    country  year      pop
# 0  Albania  1952  1282697
# 1  Albania  1957  1476505
# 2  Albania  1962  1728137
# 3  Albania  1967  1984060
```

---

## 3. 전체 흐름 — 왕복 변환 예제

```python
import pandas as pd

# ── STEP 0: 원본 (long format) ───────────────────────────
# ss : country / year / pop

print(ss.shape)   # (1704, 3)  — 행이 많음

# ── STEP 1: long → wide  (pivot_table) ──────────────────
wide = ss.pivot_table(
    index='country',
    columns='year',
    values='pop',
    aggfunc='sum'
)
wide.columns.name = None
wide = wide.reset_index()

print("=== wide format ===")
print(wide.shape)     # (142, 13)  나라 142개, year 12개 + country
print(wide.head(2))

# ── STEP 2: wide → long  (melt, 원복) ───────────────────
long = wide.melt(
    id_vars='country',
    var_name='year',
    value_name='pop'
)

# 타입 복원: year 컬럼이 문자열로 바뀌는 경우
long['year'] = long['year'].astype(int)

# 원본과 동일한 정렬
long = long.sort_values(['country', 'year']).reset_index(drop=True)

print("=== long format (원복) ===")
print(long.shape)     # (1704, 3)  — 원본과 동일
print(long.head(5))
```

---

## 4. 활용 패턴

### 패턴 A — 연도별 증감 계산 (wide에서 유리)

```python
wide = ss.pivot_table(index='country', columns='year', values='pop', aggfunc='sum')
wide.columns.name = None

year_cols = [c for c in wide.columns]

# 연도별 차분 (전년 대비 증감)
diff = wide[year_cols].diff(axis=1)
print(diff.head())
```

### 패턴 B — 시각화·모델링 (long에서 유리)

```python
import matplotlib.pyplot as plt

# long format이 seaborn/plotly에 바로 적합
long = ss.copy()

# 특정 나라 필터 후 시각화
subset = long[long['country'].isin(['Korea, Rep.', 'Japan', 'China'])]

subset.pivot_table(index='year', columns='country', values='pop').plot()
plt.title('인구 추이')
plt.show()
```

### 패턴 C — 여러 value 컬럼 melt

```python
# gdp / lifeExp / pop 세 컬럼을 동시에 melt
df_wide = pd.DataFrame({
    'country': ['Albania', 'Algeria'],
    'gdp_1952': [1601, 2449],
    'gdp_1957': [1942, 3014],
    'pop_1952': [1282697, 9279525],
    'pop_1957': [1476505, 10270856],
})

long2 = df_wide.melt(
    id_vars='country',
    value_vars=['gdp_1952', 'gdp_1957', 'pop_1952', 'pop_1957'],
    var_name='metric_year',
    value_name='value'
)
print(long2)
```

---

## 5. 핵심 요약

| 항목 | `pivot_table` | `melt` |
|---|---|---|
| **방향** | long → **wide** | wide → **long** |
| **핵심 파라미터** | `index`, `columns`, `values`, `aggfunc` | `id_vars`, `var_name`, `value_name` |
| **중복 처리** | `aggfunc`으로 집계 | 해당 없음 |
| **결과 형태** | 열이 늘어남 | 행이 늘어남 |
| **주요 용도** | 리포트, 크로스탭, 비교 | 분석, 시각화, ML 전처리 |
| **시험 포인트** | 집계 함수 종류 (`sum/mean/count`) | `id_vars` 설정이 핵심 |

> **실무 팁**: 원본 데이터는 항상 **long format**으로 보관하고,  
> 필요할 때만 `pivot_table`로 wide로 변환하는 것이 유지보수에 유리합니다.
