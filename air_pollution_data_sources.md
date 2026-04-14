# 🌍 국가별·연도별 대기오염 데이터 소스 총정리

> **작성일**: 2026년 4월 14일  
> **목적**: 국가별·연도별 대기오염 분석을 위한 공개 데이터 소스 및 접근 방법 정리  
> **주요 지표**: PM2.5, PM10, NO2, SO2, O3, CO, AQI

---

## 📋 목차

1. [WHO 세계 대기질 데이터베이스](#1-who-세계-대기질-데이터베이스)
2. [World Bank Open Data](#2-world-bank-open-data)
3. [OpenAQ](#3-openaq)
4. [Our World in Data](#4-our-world-in-data)
5. [NASA Earthdata](#5-nasa-earthdata)
6. [State of Global Air](#6-state-of-global-air)
7. [Kaggle 정제 데이터셋](#7-kaggle-정제-데이터셋)
8. [WAQI (실시간)](#8-waqi-실시간)
9. [목적별 추천 조합](#9-목적별-추천-조합)
10. [Python 데이터 수집 코드 예시](#10-python-데이터-수집-코드-예시)

---

## 1. WHO 세계 대기질 데이터베이스

| 항목 | 내용 |
|------|------|
| **URL** | https://www.who.int/data/gho/data/themes/air-pollution/who-air-quality-database |
| **제공 기관** | 세계보건기구 (WHO) |
| **갱신 주기** | 2~3년 (2011, 2016, 2018, 2021, 2022년 발표) |
| **형식** | Excel (.xlsx), CSV |
| **비용** | 무료 |
| **계정 필요** | 불필요 |

### 수록 지표

- PM2.5 연간 평균 농도 (μg/m³)
- PM10 연간 평균 농도 (μg/m³)
- NO2 연간 평균 농도 (μg/m³)

### 커버리지

- **국가**: 100개국 이상
- **도시·지역**: 6,000개 이상
- **기간**: 측정 연도별 스냅샷 (연속 시계열 아님)

### 특징 및 활용

- SDG 지표 11.6.2 (도시 대기질) 공식 데이터
- 각국 정부 보고서·공식 모니터링망 기반
- 도시 평균값으로 인구 노출 대표성 확보
- 국가·도시 공식 비교에 가장 적합

### 주의사항

> 연속 시계열이 아닌 "조사 시점" 데이터이므로, 연도별 추세 분석에는 World Bank 또는 IHME 데이터와 병행 사용 권장

---

## 2. World Bank Open Data

| 항목 | 내용 |
|------|------|
| **URL** | https://data.worldbank.org/indicator/EN.ATM.PM25.MC.M3 |
| **지표 코드** | `EN.ATM.PM25.MC.M3` |
| **제공 기관** | World Bank / IHME GBD Study |
| **갱신 주기** | 연 1회 |
| **형식** | CSV, Excel, JSON, API |
| **비용** | 무료 |
| **계정 필요** | 불필요 (API 키 불필요) |

### 수록 지표

- PM2.5 연평균 노출 농도 — 인구 가중 평균 (μg/m³)

### 커버리지

- **국가**: 전 세계 200개국 이상
- **기간**: 1990 ~ 2021년 (연속 시계열)

### API 접근

```
# 전체 국가 PM2.5 데이터 (JSON)
https://api.worldbank.org/v2/country/all/indicator/EN.ATM.PM25.MC.M3
  ?format=json&per_page=20000&mrv=30

# 특정 국가 (한국: KOR)
https://api.worldbank.org/v2/country/KOR/indicator/EN.ATM.PM25.MC.M3
  ?format=json&per_page=50

# CSV 다운로드
https://api.worldbank.org/v2/en/indicator/EN.ATM.PM25.MC.M3
  ?downloadformat=csv
```

### Python 활용

```python
import pandas as pd

# pandas_datareader로 직접 로드
import pandas_datareader.wb as wb
df = wb.download(
    indicator='EN.ATM.PM25.MC.M3',
    country='all',
    start=1990, end=2021
)
```

### 특징 및 활용

- **연도별 국가 비교에 가장 적합** (30년 연속 시계열)
- 위성 원격탐사 + 지상 측정 + 통계 모델 결합
- 국가 단위 집계값 (도시별 세분화 불가)
- 다른 World Bank 지표 (GDP, 인구, 에너지 등)와 조인 분석 용이

---

## 3. OpenAQ

| 항목 | 내용 |
|------|------|
| **URL** | https://openaq.org |
| **API** | https://api.openaq.org/v3 |
| **제공 기관** | OpenAQ (비영리 NGO) |
| **갱신 주기** | 실시간 (시간 단위) |
| **형식** | JSON (API), CSV (Explorer), AWS S3 |
| **비용** | 무료 |
| **계정 필요** | API Key 필요 (무료 발급) |

### 수록 지표

- PM2.5, PM10, O3, NO2, SO2, CO, BC (블랙카본)

### 커버리지

- **국가**: 100개국 이상
- **측정소**: 10,000개 이상
- **데이터 소스**: 정부·연구기관 100개 이상
- **기간**: 2015년~ 현재 (과거 이력 포함)

### API 접근

```
# 국가별 측정소 목록
https://api.openaq.org/v3/locations?country=KR

# 특정 기간 측정값 (CSV)
https://api.openaq.org/v2/measurements
  ?country=KR
  &parameter=pm25
  &date_from=2023-01-01
  &date_to=2023-12-31
  &format=csv
  &limit=10000
```

### AWS S3 대용량 접근

```bash
# 전체 과거 데이터 (파케이 형식)
aws s3 ls s3://openaq-data-archive/records/csv.gz/ --no-sign-request
```

### 특징 및 활용

- 측정소 단위 시간별 원시 데이터 제공 (가장 세밀한 해상도)
- 실시간 모니터링 및 과거 이력 모두 지원
- 오픈소스 플랫폼, API 무료 제공
- 도시·지역 수준 공간 분석에 적합

---

## 4. Our World in Data

| 항목 | 내용 |
|------|------|
| **URL** | https://ourworldindata.org/air-pollution |
| **제공 기관** | Our World in Data (Oxford 기반 비영리) |
| **원천 데이터** | IHME Global Burden of Disease (GBD) |
| **형식** | CSV (차트별 다운로드), API |
| **비용** | 무료 |
| **계정 필요** | 불필요 |

### 수록 지표

- 대기오염 기인 사망자 수 (연간)
- 대기오염 기인 사망률 (인구 10만명당)
- DALY (장애보정생존연수)
- PM2.5 노출 농도
- 실내·실외 오염 구분 통계

### 커버리지

- **국가**: 204개국
- **기간**: 1990 ~ 2023년

### CSV 직접 다운로드

```
# 예시: 국가별 PM2.5 노출 데이터
https://ourworldindata.org/grapher/PM25-air-pollution.csv

# 대기오염 사망률
https://ourworldindata.org/grapher/death-rate-by-source-from-air-pollution.csv
```

### 특징 및 활용

- **건강 영향(사망·DALY) 분석에 특화**
- 인터랙티브 차트에서 국가·지역 선택 후 CSV 즉시 다운로드
- 데이터 출처·방법론 상세 문서화
- 비전문가도 이해 가능한 설명 포함

---

## 5. NASA Earthdata

| 항목 | 내용 |
|------|------|
| **URL** | https://www.earthdata.nasa.gov/topics/atmosphere/air-quality |
| **제공 기관** | NASA / SEDAC |
| **형식** | NetCDF, HDF5, GeoTIFF |
| **비용** | 무료 |
| **계정 필요** | Earthdata Login 필요 (무료) |

### 주요 데이터셋

| 데이터셋 | 지표 | 해상도 |
|---------|------|--------|
| SEDAC PM2.5 | PM2.5 연평균 | 0.01° × 0.01° |
| MERRA-2 | 다종 대기오염물질 | 시간별 |
| TEMPO | NO2, O3 | 북미 고해상도 |
| MODIS MAIAC | AOD (에어로졸) | 1km |

### 특징 및 활용

- **위성 원격탐사 기반 공간 데이터** (GIS 분석 필수)
- 지상 관측소가 없는 지역도 커버
- Python `xarray`, `netCDF4` 라이브러리로 처리
- 격자(Grid) 단위 고해상도 분석에 적합

```python
import netCDF4 as nc
import numpy as np

ds = nc.Dataset('V4NA03_PM25_NA_200001_200012-RH35-NoNegs.nc')
pm25 = ds.variables['PM25'][:]  # 위도·경도·시간 배열
```

---

## 6. State of Global Air

| 항목 | 내용 |
|------|------|
| **URL** | https://www.stateofglobalair.org/data |
| **제공 기관** | Health Effects Institute (HEI) |
| **원천 데이터** | IHME GBD Study |
| **형식** | CSV (커스텀 쿼리 후 다운로드) |
| **비용** | 무료 |
| **계정 필요** | 불필요 |

### 수록 지표

- PM2.5, 오존 노출 농도
- 가정용 연소 오염 (household air pollution)
- 조기 사망자 수 (원인별)
- DALY

### 커버리지

- **국가**: 204개국
- **기간**: 1990년 ~ 최신

### 특징 및 활용

- 인터랙티브 지도·그래프로 시각적 탐색 후 데이터 내보내기
- 오염원(PM2.5, 오존, 가정용)별 건강 영향 분리 분석 가능
- 보고서·정책 자료 작성에 최적화된 포맷

---

## 7. Kaggle 정제 데이터셋

분석 연습·프로토타이핑에 즉시 활용 가능한 가공 데이터셋 모음

| 데이터셋 | URL | 특징 |
|---------|-----|------|
| Global Air Pollution Dataset | https://www.kaggle.com/datasets/hasibalmuzdadid/global-air-pollution-dataset | AQI·원인물질 포함, 국가별 |
| Global Air Pollution Data | https://www.kaggle.com/datasets/sazidthe1/global-air-pollution-data | 연도별 트렌드, 정제 완료 |
| OpenAQ on Kaggle | https://www.kaggle.com/datasets/open-aq/openaq | OpenAQ 원본 스냅샷 |
| World Air Quality Index | https://www.kaggle.com/datasets/adityaramachandran27/world-air-quality-index-by-city-and-coordinates | 도시·좌표 포함 |

### Kaggle API 다운로드

```bash
pip install kaggle
kaggle datasets download hasibalmuzdadid/global-air-pollution-dataset
```

---

## 8. WAQI (실시간)

| 항목 | 내용 |
|------|------|
| **URL** | https://waqi.info |
| **API** | https://aqicn.org/api |
| **갱신 주기** | 시간 단위 실시간 |
| **형식** | JSON (API) |
| **비용** | 무료 (API Key 발급 필요) |

### API 접근

```
# 도시별 실시간 AQI
https://api.waqi.info/feed/seoul/?token=YOUR_API_KEY

# 좌표 기반 조회
https://api.waqi.info/feed/geo:37.5665;126.9780/?token=YOUR_API_KEY
```

### 특징 및 활용

- 전 세계 10,000개 이상 측정소 실시간 데이터
- PM2.5·PM10·O3·NO2·SO2·CO AQI 통합 제공
- 실시간 대시보드·알림 시스템 구축에 적합
- 과거 이력은 제한적 (유료 플랜 필요)

---

## 9. 목적별 추천 조합

### 📊 연도별 국가 비교 분석

```
World Bank (EN.ATM.PM25.MC.M3)
  → 30년 연속 시계열, 200개국, API 제공
  + Our World in Data
  → 건강 영향 데이터 병합
```

### 🏙️ 도시·지역 수준 상세 분석

```
WHO Air Quality Database
  → 공식 도시별 연평균값
  + OpenAQ API
  → 측정소 수준 원시 데이터
```

### 🛰️ 공간(GIS) 분석

```
NASA Earthdata (SEDAC PM2.5)
  → 위성 기반 격자 데이터
  + MODIS MAIAC AOD
  → 에어로졸 공간 분포
```

### 💊 건강 영향 분석

```
State of Global Air
  → 사망·DALY 원인별 분리
  + Our World in Data
  → 시각화·설명 자료
```

### ⚡ 실시간 모니터링

```
OpenAQ API (v3)
  → 측정소 실시간 데이터
  + WAQI API
  → 통합 AQI 실시간 표출
```

### 🎓 분석 연습·프로토타이핑

```
Kaggle 정제 데이터셋
  → 즉시 활용 가능, 전처리 완료
```

---

## 10. Python 데이터 수집 코드 예시

### World Bank API 수집

```python
import requests
import pandas as pd

def fetch_worldbank_pm25(countries='all', start=1990, end=2021):
    """World Bank PM2.5 데이터 수집"""
    url = (
        f"https://api.worldbank.org/v2/country/{countries}"
        f"/indicator/EN.ATM.PM25.MC.M3"
        f"?format=json&per_page=20000&date={start}:{end}"
    )
    resp = requests.get(url)
    data = resp.json()

    records = []
    for item in data[1]:
        if item['value'] is not None:
            records.append({
                'country': item['country']['value'],
                'country_code': item['countryiso3code'],
                'year': int(item['date']),
                'pm25': item['value']
            })

    return pd.DataFrame(records).sort_values(['country', 'year'])


df = fetch_worldbank_pm25()
print(df.head())
# 피벗 테이블로 변환 (국가 × 연도)
pivot = df.pivot(index='country', columns='year', values='pm25')
```

### OpenAQ API 수집

```python
import requests
import pandas as pd

def fetch_openaq(country_code='KR', parameter='pm25',
                 date_from='2023-01-01', date_to='2023-12-31',
                 limit=10000):
    """OpenAQ 측정 데이터 수집"""
    url = "https://api.openaq.org/v2/measurements"
    params = {
        'country': country_code,
        'parameter': parameter,
        'date_from': date_from,
        'date_to': date_to,
        'limit': limit,
        'format': 'json'
    }
    headers = {'X-API-Key': 'YOUR_API_KEY'}
    resp = requests.get(url, params=params, headers=headers)
    data = resp.json()

    records = []
    for item in data['results']:
        records.append({
            'location': item['location'],
            'city': item.get('city', ''),
            'country': item['country'],
            'datetime': item['date']['utc'],
            'parameter': item['parameter'],
            'value': item['value'],
            'unit': item['unit'],
            'lat': item['coordinates']['latitude'],
            'lon': item['coordinates']['longitude']
        })

    return pd.DataFrame(records)


df_kr = fetch_openaq(country_code='KR', parameter='pm25')
```

### 데이터 전처리 및 분석 예시

```python
import pandas as pd
import numpy as np
from scipy import stats

# World Bank 데이터 로드
df = fetch_worldbank_pm25()

# 1. 기초 통계
summary = df.groupby('country')['pm25'].agg(
    ['mean', 'std', 'min', 'max']
).round(2)

# 2. 연도별 변화율
df_sorted = df.sort_values(['country', 'year'])
df_sorted['yoy_change'] = df_sorted.groupby('country')['pm25'].pct_change() * 100

# 3. 30년 추세선 (선형회귀)
def trend_slope(series):
    x = np.arange(len(series))
    slope, _, r, _, _ = stats.linregress(x, series)
    return round(slope, 3)

trends = df.groupby('country')['pm25'].apply(trend_slope).reset_index()
trends.columns = ['country', 'annual_slope_ug_m3']

# 4. 1990 vs 최신 변화율
df_1990 = df[df['year'] == 1990][['country', 'pm25']].rename(columns={'pm25': 'pm25_1990'})
df_2021 = df[df['year'] == 2021][['country', 'pm25']].rename(columns={'pm25': 'pm25_2021'})
change = pd.merge(df_1990, df_2021, on='country')
change['change_pct'] = ((change['pm25_2021'] - change['pm25_1990']) / change['pm25_1990'] * 100).round(1)

print(change.sort_values('change_pct'))
```

---

## 📌 요약 비교표

| 데이터 소스 | 지표 | 기간 | 국가 수 | 형식 | 계정 | 최적 용도 |
|------------|------|------|--------|------|------|----------|
| **WHO DB** | PM2.5·PM10·NO2 | 시점별 | 100+ | Excel/CSV | 불필요 | 도시 공식 비교 |
| **World Bank** | PM2.5 | 1990~2021 | 200+ | CSV/API/JSON | 불필요 | 국가 연도별 추세 |
| **OpenAQ** | 6종 | 2015~현재 | 100+ | API/CSV | API키 | 측정소 실시간·이력 |
| **Our World in Data** | PM2.5·사망·DALY | 1990~2023 | 204 | CSV | 불필요 | 건강영향 분석 |
| **NASA Earthdata** | PM2.5·AOD 등 | 1998~현재 | 전지구 | NetCDF | 무료계정 | GIS 공간분석 |
| **State of Global Air** | PM2.5·오존·건강 | 1990~최신 | 204 | CSV | 불필요 | 건강부담 정책자료 |
| **Kaggle** | 다양 | 다양 | 다양 | CSV | 계정 | 연습·프로토타입 |
| **WAQI** | AQI 통합 | 실시간 | 80+ | JSON | API키 | 실시간 모니터링 |

---

*참고: WHO PM2.5 연간 권고 기준 — 5 μg/m³ (2021년 개정 기준) / 구 기준 10 μg/m³ (2005년)*  
*데이터 접근 URL 및 API 명세는 각 기관 정책에 따라 변경될 수 있습니다.*
