# Sample data

Random sample of **1,500 lines per month** (02/2026, 03/2026 and 04/2026) of the official
*Execução da Despesa* files from the [Portal da Transparência](https://portaldatransparencia.gov.br).

The files are kept **exactly in the original format** (latin-1 encoding, `;` separator,
`,` as decimal separator) so the ingestion and parsing logic is exercised with real data.
Full monthly files have ~55,000–62,000 lines and can be ingested with
`ingestion/ingest_despesas.py --months YYYYMM`.
