#!/usr/bin/env python3
import argparse, json, sys, os, time
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.csv as pacsv
from utils.couch import make_session, ensure_db_exists, post_bulk

# ---------- helpers: types, ids, bulk ----------
def isoformat_arrow(value):
    if isinstance(value, pa.TimestampScalar):
        return value.as_py().isoformat().replace("+00:00", "Z")
    if isinstance(value, (pa.Date32Scalar, pa.Date64Scalar, pa.Time32Scalar, pa.Time64Scalar)):
        return value.as_py().isoformat()
    return value.as_py()

def to_python_value(val): 
    if pa.types.is_null(val.type): return None
    if pa.types.is_timestamp(val.type) or pa.types.is_date(val.type) or pa.types.is_time(val.type):
        return isoformat_arrow(val)
    if pa.types.is_decimal(val.type): return str(val.as_py())  # preserve precision
    return val.as_py() 

def record_to_doc(rec: pa.StructScalar, id_col, partition):
    d = {}
    for name, val in zip(rec.type.names, rec.values()):
        d[name] = None if not val.is_valid else to_python_value(val)
    _id = str(d[id_col]) if id_col and d.get(id_col) is not None else None
    if partition and _id: d["_id"] = f"{partition}:{_id}"
    elif _id: d["_id"] = _id
    return d

# ---------- readers (yield lists of dicts of size <= batch_size) ----------
def yield_from_arrow_dataset(path, fmt, batch_size, id_col, partition):
    dataset = ds.dataset(path, format=fmt)
    for batch in dataset.to_batches():
        struct = batch.to_struct_array()
        out = []
        for rec in struct:
            out.append(record_to_doc(rec, id_col, partition))
            if len(out) >= batch_size:
                yield out; out = []
        if out: yield out

def yield_from_csv(path, batch_size, id_col, partition):
    # Streaming-ish CSV reader via Arrow
    # (reads in chunks by setting block_size via read_options if needed)
    table = pacsv.read_csv(path)  # for very large files, switch to dataset CSV
    for batch in table.to_batches():
        struct = batch.to_struct_array()
        out = []
        for rec in struct:
            out.append(record_to_doc(rec, id_col, partition))
            if len(out) >= batch_size:
                yield out; out = []
        if out: yield out

def yield_from_ndjson(path, batch_size, id_col, partition):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            d = json.loads(line)
            _id = str(d[id_col]) if id_col and d.get(id_col) is not None else None
            if partition and _id: d["_id"] = f"{partition}:{_id}"
            elif _id: d["_id"] = _id
            out.append(d)
            if len(out) >= batch_size:
                yield out; out = []
    if out: yield out

def yield_from_json_array(path, batch_size, id_col, partition):
    # Accepts either a JSON array or JSONL; detects automatically
    with open(path, "r", encoding="utf-8") as f:
        first = f.read(1)
        f.seek(0)
        if first == "[":
            data = json.load(f)
            out = []
            for d in data:
                _id = str(d[id_col]) if id_col and d.get(id_col) is not None else None
                if partition and _id: d["_id"] = f"{partition}:{_id}"
                elif _id: d["_id"] = _id
                out.append(d)
                if len(out) >= batch_size:
                    yield out; out = []
            if out: yield out
        else:
            # treat as NDJSON
            for chunk in yield_from_ndjson(path, batch_size, id_col, partition):
                yield chunk

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--db", required=True)  # http://user:pass@host:5984/mydb
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--id-col", default=None)
    ap.add_argument("--partition", default=None)
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--retries", type=int, default=5)
    ap.add_argument("--backoff", type=float, default=0.5)
    ap.add_argument("--new-edits", type=str, default=None)  # "true"/"false"
    args = ap.parse_args()

    start = time.perf_counter()
    ext = os.path.splitext(args.input)[1].lower()
    new_edits = None if args.new_edits is None else (args.new_edits.lower()=="true")

    # Choose reader
    if ext == ".parquet":
        reader = yield_from_arrow_dataset(args.input, "parquet", args.batch_size, args.id_col, args.partition)
    elif ext == ".csv":
        # for huge CSVs, prefer ds.dataset(input, format="csv") + to_batches()
        reader = yield_from_csv(args.input, args.batch_size, args.id_col, args.partition)
    elif ext in (".ndjson", ".jsonl"):
        reader = yield_from_ndjson(args.input, args.batch_size, args.id_col, args.partition)
    elif ext == ".json":
        reader = yield_from_json_array(args.input, args.batch_size, args.id_col, args.partition)
    else:
        print(f"Unsupported extension: {ext}", file=sys.stderr); sys.exit(2)

    session = make_session(args.retries, args.backoff)
    ensure_db_exists(session, args.db, args.timeout)

    total = failures = 0
    for docs in reader:
        try:
            res = post_bulk(session, args.db, docs, new_edits, args.timeout)
            total += len(res)
            failures += sum(1 for r in res if "error" in r)
            # print first error (if any) for visibility
            for r in res:
                if "error" in r:
                    print(f"[bulk-error] id={r.get('id')} err={r.get('error')} reason={r.get('reason')}", file=sys.stderr)
                    break
        except Exception as e:
            failures += len(docs)
            print(f"[request-failed] {e}", file=sys.stderr)

    duration = time.perf_counter() - start
    print(f"Imported: {total-failures} | Failures: {failures} | Elapsed: {duration:.2f}s")

if __name__ == "__main__":
    main()
