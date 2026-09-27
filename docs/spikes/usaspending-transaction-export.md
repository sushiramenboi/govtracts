# USAspending transaction export — Phase 0 evidence

## Decision

**GO with documented conditions.** Stakeholder review accepted the Phase 0 evidence and made these decisions:

1. The observed **$0.03 PSC aggregate residual** is accepted as an immaterial category-aggregation variance for this sample. This acceptance does not relax exact requirements for transaction counts, unique identifiers, date boundaries, signed amounts, or the overall net-obligation total.
2. The official `/api/v2/bulk_download/awards/` endpoint is approved as Govtracts' primary transaction-acquisition path. A successful `/api/v2/download/transactions/` result is no longer a Phase 1 prerequisite.
3. Phase 0 is approved as **GO with documented conditions**. This decision closes the two review gates recorded by the automated evidence run without changing the underlying measurements.

No production code, endpoint, table, schema, frontend, or backfill was changed or started by this spike.

## Scope and invariants

- Sample window: **2024-10-01 through 2024-10-31**, one completed federal fiscal month (FY2025 month 1).
- Award types: prime contracts `A`, `B`, `C`, and `D`.
- Date basis: `action_date`.
- Grain: transaction rows, preserving signed obligations and deobligations.
- October 2024 was never subdivided.
- A returned service limit is a reason to evaluate official full/delta archives, not a reason to create smaller date partitions solely to evade that limit.
- Phase 2 planning invariant: load one completed federal fiscal year plus the preceding fiscal year's required comparison data.

The isolated harness is `services/api/tools/usaspending_feasibility.py`; focused tests are in `services/api/tests/test_usaspending_feasibility.py`. Runtime downloads and journals remain outside the repository under `/tmp/govtracts-usaspending-phase0*`.

## Preflight and acquisition evidence

The harness posted the exact A–D/action-date filter to `/api/v2/download/count/` with `spending_level: transactions`. The service returned:

| Measure | Result |
| --- | ---: |
| Calculated transaction rows | 490,377 |
| Maximum transaction limit | 500,000 |
| `rows_gt_limit` | `false` |
| `transaction_rows_gt_limit` | `false` |

The same month and award-type filter was then sent to `/api/v2/download/transactions/`. The service accepted the request, expanded its recorded download types to both `elasticsearch_transactions` and `elasticsearch_sub_awards`, and returned this resumable job:

- File: `PrimeTransactionsAndSubawards_2026-09-27_H02M22S00901122.zip`
- Status URL: `https://api.usaspending.gov/api/v2/download/status?file_name=PrimeTransactionsAndSubawards_2026-09-27_H02M22S00901122.zip`
- Terminal result: `failed` after 385.735999 seconds, zero rows, message `An error occurred.`

That attempt requested legacy/non-export column identifiers. Inspection of USAspending's official export column lookup produced the corrected 16-column mapping used below. Because the transaction job was terminal, it could not be resumed.

The first bulk awards job was recoverable by its saved status URL, so it was resumed rather than duplicated. It had already reached a terminal failure:

- File: `All_PrimeTransactions_2026-09-27_H02M30S23240877.zip`
- Terminal result: `failed` after 1,032.574144 seconds, 297 reported columns, zero rows, message `An error occurred.`

Only after confirming that failure did the harness submit one same-month, unsplit recovery request to `/api/v2/bulk_download/awards/`, limited to the corrected 16 columns. It returned:

- File: `All_PrimeTransactions_2026-09-27_H02M53S33702415.zip`
- Status URL: `https://api.usaspending.gov/api/v2/download/status?file_name=All_PrimeTransactions_2026-09-27_H02M53S33702415.zip`
- Terminal result: `finished` after 488.4533 seconds
- Service-reported rows: 490,377
- Service-reported columns: 16

Polling used the returned status URL with bounded, increasing intervals capped at 60 seconds. The journal was checkpointed so an interrupted run could resume without creating another job.

## ZIP and CSV validation

The finished ZIP contains one CSV member, `All_Contracts_PrimeTransactions_2026-09-27_H02M53S34_1.csv`, with CRC32 `0cc77ebc`. ZIP member paths were checked before reading to prevent path traversal.

The CSV headers exactly matched the corrected requested export fields:

```text
contract_transaction_unique_key
contract_award_unique_key
award_id_piid
award_type_code
action_date
federal_action_obligation
recipient_name
recipient_uei
recipient_parent_name
recipient_parent_uei
awarding_agency_name
awarding_sub_agency_name
naics_code
product_or_service_code
transaction_description
last_modified_date
```

| Validation | Result |
| --- | ---: |
| CSV rows | 490,377 |
| Distinct stable transaction IDs | 490,377 |
| Duplicate transaction rows | 0 |
| Missing stable transaction IDs | 0 |
| Distinct generated award IDs | 479,852 |
| Invalid action dates | 0 |
| Rows outside October 2024 | 0 |
| Invalid signed obligations | 0 |
| Positive-obligation rows | 446,461 |
| Negative-obligation rows | 17,356 |
| Zero-obligation rows | 26,560 |
| Gross positive obligations | $50,400,300,875.77 |
| Signed deobligations | -$1,686,079,144.20 |
| Net obligations | $48,714,221,731.57 |

Required identifiers, display award IDs, award types, dates, amounts, agency/subagency, recipient name/UEI, description, and last-modified date had no missing values. Optional classification/parent fields had these observed null counts:

| Field | Null rows | Obligation sum on null rows |
| --- | ---: | ---: |
| NAICS | 16 | $861,602,809.78 |
| PSC | 2 | $0.00 |
| Parent recipient name | 35 | $3,216,102.46 |
| Parent recipient UEI | 53 | $3,557,127.44 |

Observed cardinalities were 4 award types, 62 awarding agencies, 144 awarding subagencies, 24,928 recipient names, 902 NAICS codes, and 1,696 PSCs.

## Aggregate reconciliation

All aggregate requests used the same A–D award types, `action_date`, and October 2024 window.

| Comparison | Aggregate total | CSV net | Raw difference | Unclassified amount | Residual |
| --- | ---: | ---: | ---: | ---: | ---: |
| Spending over time, monthly transaction level | $48,714,221,731.57 | $48,714,221,731.57 | $0.00 | n/a | $0.00 |
| Awarding agency categories | $48,714,221,731.57 | $48,714,221,731.57 | $0.00 | $0.00 | $0.00 |
| NAICS categories | $47,852,618,921.79 | $48,714,221,731.57 | $861,602,809.78 | $861,602,809.78 | $0.00 |
| PSC categories | $48,714,221,731.54 | $48,714,221,731.57 | $0.03 | $0.00 | **$0.03** |

The count endpoint, service-reported download rows, parsed rows, and distinct transaction IDs all agree exactly at 490,377. The NAICS difference is completely explained by the 16 rows without a NAICS value. The PSC difference is not explained by the two missing-PSC rows because both sum to zero. Stakeholders accepted this observed $0.03 PSC residual as an immaterial category-aggregation variance for this sample only; all transaction-level invariants and the overall net-obligation reconciliation remain exact requirements.

## Throughput and storage observations

| Measure | Result |
| --- | ---: |
| Compressed ZIP size | 35,910,571 bytes (34.25 MiB) |
| Uncompressed CSV size | 167,698,480 bytes (159.93 MiB) |
| Download time | 4.005 seconds |
| Observed transfer rate | 8,966,439 bytes/second (8.55 MiB/s) |
| Parse time | 4.195 seconds |
| Parse throughput | 116,908 rows/second |
| Modeled PostgreSQL heap | 203,572,768 bytes (194.14 MiB) |
| Modeled stable transaction ID index | 31,383,416 bytes (29.93 MiB) |
| Modeled generated award ID index | 34,666,352 bytes (33.06 MiB) |
| Modeled total | 269,622,536 bytes (257.13 MiB) |

The PostgreSQL estimate is a feasibility model based on observed UTF-8 payload plus tuple, varlena, and index-entry assumptions. It excludes page fill effects, WAL, TOAST, vacuum headroom, staging copies, and future columns; it is not a production schema or capacity commitment.

## Official historical-download alternatives

`/api/v2/bulk_download/list_monthly_files/` returned two relevant official options for contracts:

- `FY2025_All_Contracts_Full_20260906.zip`: a full FY2025 contracts archive and the preferred historical alternative if custom exports would require excessive partitioning.
- `FY(All)_All_Contracts_Delta_20260924.zip`: an all-year delta archive suitable for evaluation as a correction/update feed, but not by itself a complete historical snapshot.

For Phase 2, acquisition design must cover one completed fiscal year and the preceding fiscal year's required comparison data. It should compare those official full/delta files with custom export behavior and operational costs. It must not use date partitioning solely to circumvent a returned service limit.

## Exit recommendation

The final Phase 0 decision is **GO with documented conditions**. Whole-month acquisition through the minimal official bulk request is feasible, and the core data has the required transaction identity and signed-obligation behavior.

The documented conditions are:

1. Use `/api/v2/bulk_download/awards/` as Govtracts' primary transaction-acquisition endpoint. The unsuccessful `/api/v2/download/transactions/` observation remains part of the evidence record but is not a Phase 1 prerequisite.
2. Treat the observed $0.03 PSC difference as an immaterial category-aggregation variance for this October 2024 sample only. Do not apply that acceptance to transaction counts, unique identifiers, date boundaries, signed amounts, or the overall net-obligation total; those remain exact gates.
3. Review the sample-based PostgreSQL footprint model before relying on it for annual capacity planning, and retain the Phase 2 requirement to load one completed fiscal year plus the preceding fiscal year's required comparison data.

Phase 0 stops here. This decision does not itself begin Phase 1.
