---
layout: global
title: ORC Dictionary Encoding and orc.dictionary.key.threshold
displayTitle: ORC Dictionary Encoding and orc.dictionary.key.threshold
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

## ORC File Structure

ORC (Optimized Row Columnar) is a columnar storage format designed for Hadoop workloads.
Understanding its physical layout is essential to tuning write performance.

```
ORC File
├── Stripe 1                 ← unit of parallel read/write (~64 MB by default)
│   ├── Index data           ← row-group statistics and bloom filter entries
│   ├── Row data
│   │   ├── Column 0 streams (struct header)
│   │   ├── Column 1 streams (first real column)
│   │   │   ├── PRESENT stream   (null bitmap)
│   │   │   ├── DATA stream      (encoded values)
│   │   │   └── DICTIONARY stream (optional: unique values when dict-encoded)
│   │   └── Column N streams
│   └── Stripe footer        ← column encoding kinds, stream offsets, statistics
├── Stripe 2
│   └── ...
└── File footer              ← schema, stripe locations, file statistics
```

Each stripe is written and read independently.
Column encoding decisions (dictionary vs. direct) are **per stripe**, not per file.

---

## String Column Encodings

ORC uses Run-Length Encoding v2 (RLEv2) throughout and offers two modes for string columns:

### DIRECT_V2

The raw byte sequences of each string value are written one after another into the DATA stream,
followed by a LENGTH stream that records each string's byte length.

```
Row values:  "London", "Paris",  "Berlin", "London", "Paris"
DATA stream: L o n d o n P a r i s B e r l i n L o n d o n P a r i s
LENGTH:      6           5           6           6           5
```

Reading cost: per-row byte copy with no extra lookup.
Size: proportional to the total bytes of all values (including duplicates).

### DICTIONARY_V2

First pass: collect all unique values (the dictionary) and sort them.
Assign each unique value an integer index.
Write indices into the DATA stream using RLEv2 integer encoding.
Write the dictionary's sorted values once into the DICTIONARY_DATA stream.

```
Row values:  "London", "Paris",  "Berlin", "London", "Paris"

Dictionary (sorted):
  0 → "Berlin"
  1 → "London"
  2 → "Paris"

DATA (integer indices, RLEv2-compressed):
  1, 2, 0, 1, 2

DICTIONARY_DATA: B e r l i n L o n d o n P a r i s
LENGTH:          6           6           5
```

Reading cost: integer decode + one dictionary lookup per row.
Size: (dictionary bytes) + (index stream, usually much smaller than raw strings).

### When DICTIONARY_V2 wins

Dictionary encoding is efficient when many rows share a small set of values — for example:
country codes, status enums, city names, or any categorical column.
If every row has a unique value (like a UUID or a hash), the dictionary stores the entire dataset
twice (raw bytes + indices), which is slower to write, larger, and slower to read.

---

## orc.dictionary.key.threshold

`orc.dictionary.key.threshold` is the ORC writer configuration that controls the automatic
switch between DICTIONARY_V2 and DIRECT_V2 encoding based on the observed cardinality ratio
within each stripe.

### Decision rule

At the end of the first pass over a stripe the ORC writer knows:
- `N` — total rows in the stripe
- `K` — number of distinct string values collected in the dictionary

The check inside the ORC writer (`WriterImpl.java`) is:

```java
if (K > Math.round(N * dictionaryKeySizeThreshold)) {
    useDictionaryEncoding = false;  // → DIRECT_V2
}
```

In plain words:

```
cardinality_ratio = K / N                   (distinct / total)

if cardinality_ratio > threshold:
    encoding = DIRECT_V2   (high cardinality, dict would waste space)
else:
    encoding = DICTIONARY_V2 (low cardinality, dict compresses well)
```

### Default value

The ORC library default is **0.8** (80 %).
Spark passes writer options through directly to the ORC library via Hadoop Configuration,
so the effective default when Spark does not override the option is also 0.8.

### How to set it

**DataFrameWriter option (per write operation):**

```python
df.write \
  .option("orc.dictionary.key.threshold", "0.8") \
  .orc("/path/to/output")
```

**SQL DDL (persistent table property):**

```sql
CREATE TABLE events (
  event_type STRING,   -- low cardinality: ~20 distinct values
  session_id STRING,   -- high cardinality: ~millions of unique values
  payload    STRING
)
USING ORC
OPTIONS (
  orc.dictionary.key.threshold '0.8'
)
```

**Hive ORC tables** use a different key:

```sql
CREATE TABLE events ( ... )
STORED AS ORC
TBLPROPERTIES (
  'hive.exec.orc.dictionary.key.size.threshold' = '0.8'
)
```

---

## Encoding decision walkthrough

Consider a stripe with 10 000 rows, threshold = 0.8 (80 %):

| Column        | Distinct values | K / N  | K / N ≤ 0.8? | Encoding    |
|---------------|----------------|--------|--------------|-------------|
| country_code  | 50             | 0.5 %  | yes          | DICTIONARY_V2 |
| city_name     | 500            | 5 %    | yes          | DICTIONARY_V2 |
| event_type    | 8 000          | 80 %   | yes (=)      | DICTIONARY_V2 |
| session_uuid  | 10 000         | 100 %  | **no**       | DIRECT_V2   |

With threshold = 1.0:

| Column        | K / N  | K / N ≤ 1.0? | Encoding    |
|---------------|--------|--------------|-------------|
| session_uuid  | 100 %  | yes (=)      | DICTIONARY_V2 |

Setting threshold to 1.0 forces dictionary encoding for every string column regardless of
cardinality. This is useful when you combine it with `orc.column.encoding.direct` to opt out
only specific columns explicitly:

```sql
CREATE TABLE t (
  country STRING,          -- will be DICTIONARY_V2
  session_id STRING,       -- will be DIRECT_V2 (explicit override)
  product_name STRING      -- will be DICTIONARY_V2
)
USING ORC
OPTIONS (
  orc.dictionary.key.threshold '1.0',
  orc.column.encoding.direct   'session_id'
)
```

---

## Inspecting the actual encoding of an ORC file

You can verify which encoding was chosen for each column using the ORC tools JAR
(`orc-tools-<version>-uber.jar`) available from [orc.apache.org](https://orc.apache.org):

```bash
java -jar orc-tools-1.9.8-uber.jar meta /path/to/file.orc
```

Look for lines such as:

```
Stripe: offset=3 data=1234567 rows=10000 tail=192 index=1024
  Stream: column 1 section DATA start: 3 length 1200
  Stream: column 1 section DICTIONARY_DATA start: 1203 length 800
  Encoding column 1: DICTIONARY_V2 dictionarySize: 50
  Encoding column 2: DIRECT_V2
```

From Spark you can read the encoding kind directly via the ORC reader API
(as done in `OrcSourceSuite`):

```scala
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcFile, OrcProto}
import org.apache.orc.impl.RecordReaderImpl

val reader = OrcFile.createReader(new Path("/path/to/file.orc"),
  OrcFile.readerOptions(new Configuration()))
val rr = reader.rows.asInstanceOf[RecordReaderImpl]
val stripe = rr.readStripeFooter(reader.getStripes.get(0))

// Column indices: 0 = top-level struct, 1 = first column, 2 = second, …
stripe.getColumnsList.asScala.zipWithIndex.foreach { case (col, i) =>
  println(s"column $i: ${col.getKind}")
}
```

---

## Tuning guide

### Low-cardinality columns (enums, country codes, status fields)

These benefit most from dictionary encoding. The default 0.8 already handles them.
No change needed unless you want to guarantee dictionary is always used:

```
orc.dictionary.key.threshold = 1.0
```

### High-cardinality columns (UUIDs, hashes, free-text)

The default 0.8 will automatically fall back to DIRECT_V2 if > 80 % of values are distinct.
If you need to force DIRECT regardless of cardinality, use the explicit column override:

```
orc.column.encoding.direct = 'col1,col2'
```

This is cheaper than relying on the threshold check because ORC skips the dictionary-building
pass entirely for those columns.

### Mixed workloads (most tables)

Keep the default **0.8** and use `orc.column.encoding.direct` for known high-cardinality
columns. This avoids spending time building dictionaries that will be abandoned anyway.

### Benchmark reference

In `FilterPushdownBenchmark.scala` the parameter is used as a binary switch:

```scala
.option("orc.dictionary.key.threshold", if (useDictionary) 1.0 else 0.8)
```

- `1.0` → dictionary guaranteed for all string columns (used when measuring filter-pushdown
  speed on dict-encoded data, where predicate evaluation is faster due to integer comparisons)
- `0.8` → natural fall-through to direct for the deliberately high-cardinality benchmark column

---

## Summary

| threshold | Effect |
|-----------|--------|
| `0.0`     | All string columns use DIRECT_V2 (dictionary is never built) |
| `0.8`     | Default. Dictionary used when ≤ 80 % of values are distinct per stripe |
| `1.0`     | Dictionary always used; opt out per-column with `orc.column.encoding.direct` |

The parameter only controls the **automatic fallback** decision.
An explicit `orc.column.encoding.direct` override takes precedence over the threshold.
