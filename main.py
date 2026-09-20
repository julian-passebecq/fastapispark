from __future__ import annotations

import hmac
import json
import math
import os
import re
import uuid
from enum import Enum
from typing import Any

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator, model_validator

from real_spark import RealSparkOracle

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
FORBIDDEN_SQL = re.compile(
    r"\b(attach|copy|export|import|install|load|pragma|call|create|drop|alter|delete|update|insert|"
    r"merge|replace|truncate|vacuum|checkpoint|read_csv|read_json|read_parquet|parquet_scan|"
    r"httpfs|sqlite_scan|postgres_scan)\b",
    re.IGNORECASE,
)

class OperationType(str, Enum):
    FILTER = "filter"
    SELECT = "select"
    WITH_COLUMN = "with_column"
    GROUP_BY = "group_by"
    ORDER_BY = "order_by"
    LIMIT = "limit"
    DISTINCT = "distinct"
    DROP_DUPLICATES = "drop_duplicates"
    REPARTITION = "repartition"
    JOIN = "join"

class Operation(BaseModel):
    op: OperationType
    args: dict[str, Any] = Field(default_factory=dict)

class TableData(BaseModel):
    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    rows: list[dict[str, Any]] = Field(default_factory=list, max_length=5000)

    @field_validator("rows")
    @classmethod
    def validate_rows(cls, rows):
        if any(len(row) > 100 for row in rows):
            raise ValueError("Each row may contain at most 100 columns")
        return rows

class SimulationHints(BaseModel):
    input_rows: int | None = Field(default=None, ge=0, le=10_000_000_000)
    input_bytes: int | None = Field(default=None, ge=0, le=100_000_000_000_000)
    partitions: int | None = Field(default=None, ge=1, le=100_000)
    skew_factor: float = Field(default=1.0, ge=1.0, le=20.0)
    filter_selectivity: float = Field(default=0.35, gt=0.0, le=1.0)
    group_cardinality_ratio: float = Field(default=0.08, gt=0.0, le=1.0)

class CompileRequest(BaseModel):
    code: str = Field(min_length=1, max_length=20_000)

class PlanRequest(BaseModel):
    runtime_id: str = "datapass-free"
    source_table: str = Field(default="input", pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    operations: list[Operation] = Field(default_factory=list, max_length=50)
    tables: list[TableData] = Field(default_factory=list, max_length=8)
    hints: SimulationHints = Field(default_factory=SimulationHints)

class ExecuteRequest(PlanRequest):
    collect_limit: int = Field(default=100, ge=1, le=500)

    @model_validator(mode="after")
    def source_exists(self):
        if self.tables and self.source_table not in {t.name for t in self.tables}:
            raise ValueError(f"source_table '{self.source_table}' is not present in tables")
        return self

class SqlRequest(BaseModel):
    runtime_id: str = "datapass-free"
    sql: str = Field(min_length=1, max_length=20_000)
    tables: list[TableData] = Field(default_factory=list, max_length=8)
    collect_limit: int = Field(default=100, ge=1, le=500)
    hints: SimulationHints = Field(default_factory=SimulationHints)

class VerifyTableData(BaseModel):
    name: str = Field(min_length=1, max_length=129, pattern=r'^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?    collect_limit: int = Field(default=100, ge=1, le=200)

    @model_validator(mode="after")
    def bounded_fixture(self):
        if sum(len(table.rows) for table in self.tables) > 5000:
            raise ValueError("Real Spark verification allows at most 5000 fixture rows total")
        return self


def require_runner_key(x_datapass_runner_key: str | None = Header(default=None)):
    expected = os.getenv("DATAPASS_RUNNER_KEY")
    if not expected:
        raise HTTPException(503, "Real Spark verification is disabled until DATAPASS_RUNNER_KEY is configured.")
    if not x_datapass_runner_key or not hmac.compare_digest(x_datapass_runner_key, expected):
        raise HTTPException(401, "Invalid Datapass runner key.")


RUNTIMES = {
    "datapass-free": dict(label="Datapass Free Lab", executors=1, cores=2, memory_gb=1.0, partitions=4, scan=180, shuffle=90, rows_sec=180000, startup=80, credits_hour=0.0),
    "datapass-s": dict(label="Datapass S", executors=2, cores=2, memory_gb=2.0, partitions=8, scan=420, shuffle=190, rows_sec=240000, startup=120, credits_hour=0.06),
    "datapass-m": dict(label="Datapass M", executors=4, cores=4, memory_gb=4.0, partitions=32, scan=1050, shuffle=520, rows_sec=310000, startup=180, credits_hour=0.18),
    "datapass-l": dict(label="Datapass L", executors=8, cores=4, memory_gb=8.0, partitions=64, scan=2100, shuffle=1100, rows_sec=360000, startup=260, credits_hour=0.42),
}

def qident(value: str) -> str:
    if not IDENT.fullmatch(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return f'"{value}"'

def validate_expr(text: str) -> str:
    text = text.strip()
    if not text or ";" in text or "--" in text or "/*" in text or "*/" in text or FORBIDDEN_SQL.search(text):
        raise ValueError("Unsafe or empty expression")
    return text

def validate_query(text: str) -> str:
    text = text.strip().rstrip(";").strip()
    if not re.match(r"^(select|with)\b", text, flags=re.I):
        raise ValueError("Only SELECT or WITH queries are allowed")
    if ";" in text or "--" in text or "/*" in text or "*/" in text or FORBIDDEN_SQL.search(text):
        raise ValueError("Only one safe in-memory query is allowed")
    return text

def compile_plan_sql(source: str, operations: list[Operation]) -> str:
    sql = f"SELECT * FROM {qident(source)}"
    for i, op in enumerate(operations):
        a = f"q{i}"
        if op.op == OperationType.FILTER:
            sql = f"SELECT * FROM ({sql}) AS {a} WHERE {validate_expr(str(op.args.get('condition','')))}"
        elif op.op == OperationType.SELECT:
            cols = op.args.get("columns") or []
            if not cols: raise ValueError("select requires columns")
            sql = f"SELECT {', '.join(qident(str(c)) for c in cols)} FROM ({sql}) AS {a}"
        elif op.op == OperationType.WITH_COLUMN:
            name = qident(str(op.args.get("name","")))
            expr = validate_expr(str(op.args.get("expression","")))
            sql = f"SELECT *, {expr} AS {name} FROM ({sql}) AS {a}"
        elif op.op == OperationType.GROUP_BY:
            keys = [qident(str(k)) for k in op.args.get("keys",[])]
            aggs = []
            for item in op.args.get("aggregations") or []:
                fn = str(item.get("function","")).lower()
                if fn not in {"sum","avg","min","max","count"}: raise ValueError("Unsupported aggregation")
                aggs.append(f"{fn.upper()}({qident(str(item['column']))}) AS {qident(str(item['alias']))}")
            if not aggs: raise ValueError("group_by requires aggregations")
            sql = f"SELECT {', '.join(keys+aggs)} FROM ({sql}) AS {a}" + (f" GROUP BY {', '.join(keys)}" if keys else "")
        elif op.op == OperationType.ORDER_BY:
            terms=[]
            for item in op.args.get("columns") or []:
                d=str(item.get("direction","asc")).lower()
                if d not in {"asc","desc"}: raise ValueError("direction must be asc or desc")
                terms.append(f"{qident(str(item['column']))} {d.upper()}")
            if not terms: raise ValueError("order_by requires columns")
            sql=f"SELECT * FROM ({sql}) AS {a} ORDER BY {', '.join(terms)}"
        elif op.op == OperationType.LIMIT:
            n=int(op.args.get("count",0))
            if n < 0 or n > 1_000_000: raise ValueError("limit out of range")
            sql=f"SELECT * FROM ({sql}) AS {a} LIMIT {n}"
        elif op.op == OperationType.DISTINCT:
            sql=f"SELECT DISTINCT * FROM ({sql}) AS {a}"
        elif op.op == OperationType.DROP_DUPLICATES:
            keys=op.args.get("keys") or []
            if not keys: sql=f"SELECT DISTINCT * FROM ({sql}) AS {a}"
            else:
                qq=", ".join(qident(str(k)) for k in keys)
                sql=f"SELECT * EXCLUDE (__rn) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {qq}) AS __rn FROM ({sql}) AS {a}) WHERE __rn=1"
        elif op.op == OperationType.REPARTITION:
            pass
        elif op.op == OperationType.JOIN:
            right=qident(str(op.args.get("right_table","")))
            keys=op.args.get("on") or []
            how=str(op.args.get("how","inner")).lower()
            jmap={"inner":"INNER","left":"LEFT","right":"RIGHT","full":"FULL"}
            if not keys or how not in jmap: raise ValueError("Invalid join")
            la,ra=f"l{i}",f"r{i}"
            pred=" AND ".join(f"{la}.{qident(str(k))}={ra}.{qident(str(k))}" for k in keys)
            sql=f"SELECT {la}.*, {ra}.* EXCLUDE ({', '.join(qident(str(k)) for k in keys)}) FROM ({sql}) AS {la} {jmap[how]} JOIN {right} AS {ra} ON {pred}"
    return sql

def register_table(con, table: TableData):
    cols=[]
    seen=set()
    for row in table.rows:
        for k in row:
            qident(k)
            if k not in seen: seen.add(k); cols.append(k)
    if not cols:
        con.execute(f"CREATE TABLE {qident(table.name)} (__empty INTEGER)")
        return
    def dtype(values):
        vals=[v for v in values if v is not None]
        if not vals: return "VARCHAR"
        if all(isinstance(v,bool) for v in vals): return "BOOLEAN"
        if all(isinstance(v,int) and not isinstance(v,bool) for v in vals): return "BIGINT"
        if all(isinstance(v,(int,float)) and not isinstance(v,bool) for v in vals): return "DOUBLE"
        return "VARCHAR"
    defs=", ".join(f"{qident(c)} {dtype([r.get(c) for r in table.rows])}" for c in cols)
    con.execute(f"CREATE TABLE {qident(table.name)} ({defs})")
    if table.rows:
        con.executemany(
            f"INSERT INTO {qident(table.name)} VALUES ({', '.join(['?']*len(cols))})",
            [[r.get(c) if r.get(c) is None or isinstance(r.get(c),(str,int,float,bool)) else str(r.get(c)) for c in cols] for r in table.rows],
        )

def execute_sql(tables: list[TableData], sql: str, limit: int):
    con=duckdb.connect(":memory:")
    for stmt in ("SET enable_external_access=false","SET autoinstall_known_extensions=false","SET autoload_known_extensions=false"):
        try: con.execute(stmt)
        except Exception: pass
    for t in tables: register_table(con,t)
    try:
        cur=con.execute(f"SELECT * FROM ({validate_query(sql)}) AS datapass_result LIMIT {limit+1}")
        cols=[d[0] for d in cur.description]
        raw=cur.fetchall()
    finally:
        con.close()
    return cols,[dict(zip(cols,row,strict=True)) for row in raw[:limit]],len(raw)>limit

def simulate(runtime_id: str, source: str, ops: list[Operation], tables: list[TableData], hints: SimulationHints):
    if runtime_id not in RUNTIMES: raise ValueError("Unknown runtime_id")
    rt=RUNTIMES[runtime_id]
    table=next((t for t in tables if t.name==source),None)
    obs_rows=len(table.rows) if table else 100000
    obs_bytes=max(sum(len(json.dumps(r,default=str).encode()) for r in table.rows),obs_rows*16) if table else 12_000_000
    rows=hints.input_rows if hints.input_rows is not None else obs_rows
    size=hints.input_bytes if hints.input_bytes is not None else obs_bytes
    parts=hints.partitions or rt["partitions"]
    stages=[]; warnings=[]; current_ops=[]; pending=0
    stage_rows=rows; stage_bytes=size
    wide={OperationType.GROUP_BY,OperationType.ORDER_BY,OperationType.DISTINCT,OperationType.DROP_DUPLICATES,OperationType.REPARTITION,OperationType.JOIN}
    def finish(reason, sw=0):
        nonlocal current_ops,pending,stage_rows,stage_bytes
        if not current_ops and not stages: current_ops=["scan"]
        cores=rt["executors"]*rt["cores"]
        duration=max(1,int((rt["startup"]/1000 + stage_bytes/(rt["scan"]*1024*1024) + (pending+sw)/(rt["shuffle"]*1024*1024) + stage_rows/(rt["rows_sec"]*cores) + parts*0.0025/cores)*1000*(1+(hints.skew_factor-1)*0.22)))
        budget=rt["executors"]*rt["memory_gb"]*1024**3*0.62
        spill=max(0,int((stage_bytes+pending+sw-budget)*0.55))
        stages.append(dict(stage_id=len(stages),reason=reason,operations=list(current_ops),tasks=max(1,parts),input_rows=max(0,stage_rows),output_rows=max(0,rows),input_bytes=max(0,stage_bytes),shuffle_read_bytes=max(0,pending),shuffle_write_bytes=max(0,sw),spill_bytes=spill,duration_ms=duration,skew_factor=hints.skew_factor))
        if spill: warnings.append("Simulated spill detected: executor memory is undersized for this stage.")
        current_ops=[]; pending=sw; stage_rows=rows; stage_bytes=size
    for op in ops:
        current_ops.append(op.op.value)
        if op.op==OperationType.FILTER: rows=int(rows*hints.filter_selectivity); size=int(size*hints.filter_selectivity)
        elif op.op==OperationType.SELECT: size=int(size*min(1,max(.12,len(op.args.get("columns",[]))/10 or .5)))
        elif op.op==OperationType.WITH_COLUMN: size=int(size*1.08)
        elif op.op==OperationType.LIMIT:
            n=int(op.args.get("count",rows)); ratio=min(1,n/max(rows,1)); rows=min(rows,n); size=int(size*ratio)
        elif op.op==OperationType.GROUP_BY: rows=max(1,int(rows*hints.group_cardinality_ratio)) if rows else 0; size=max(rows*48,int(size*.18))
        elif op.op in {OperationType.DISTINCT,OperationType.DROP_DUPLICATES}: rows=int(rows*.7); size=int(size*.72)
        elif op.op==OperationType.JOIN: rows=int(rows*1.12); size=int(size*1.25)
        elif op.op==OperationType.REPARTITION: parts=max(1,int(op.args.get("partitions",parts)))
        if op.op in wide:
            ratios={OperationType.GROUP_BY:.68,OperationType.ORDER_BY:1,OperationType.DISTINCT:.82,OperationType.DROP_DUPLICATES:.82,OperationType.REPARTITION:1,OperationType.JOIN:.92}
            sw=int(size*(.08 if op.op==OperationType.JOIN and op.args.get("broadcast") else ratios[op.op]))
            finish(f"shuffle boundary: {op.op.value}",sw)
            if op.op==OperationType.GROUP_BY: warnings.append("groupBy creates a wide dependency; inspect key cardinality and skew.")
            if op.op==OperationType.ORDER_BY: warnings.append("Global orderBy/sort requires a shuffle.")
            if op.op==OperationType.REPARTITION: warnings.append("repartition() forces a full shuffle.")
    if current_ops or not stages: finish("result stage")
    total_ms=sum(s["duration_ms"] for s in stages)
    if hints.skew_factor>=2: warnings.append("High simulated skew factor: a minority of partitions dominate runtime.")
    return dict(
        runtime_id=runtime_id,total_duration_ms=total_ms,total_tasks=sum(s["tasks"] for s in stages),
        total_shuffle_bytes=sum(s["shuffle_read_bytes"]+s["shuffle_write_bytes"] for s in stages),
        total_spill_bytes=sum(s["spill_bytes"] for s in stages),
        simulated_credits=round((total_ms/3_600_000)*rt["credits_hour"],6),
        stages=stages,warnings=list(dict.fromkeys(warnings)),
        disclaimer="Distributed metrics and Datapass credits are simulations for learning; they are not measurements or vendor pricing."
    )

def compile_pyspark(code: str):
    source=None; ops=[]; warnings=[]
    lines=[x.strip() for x in code.splitlines() if x.strip() and not x.strip().startswith("#")]
    for line in lines:
        m=re.search(r'spark\.table\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
        if m: source=m.group(2); continue
        if ".filter(" in line or ".where(" in line:
            m=re.search(r'\.(?:filter|where)\((.+)\)\s*$',line)
            if not m: raise ValueError(f"Could not parse filter: {line}")
            expr=m.group(1).strip()
            if len(expr)>=2 and expr[0] in {'"',"'"} and expr[-1]==expr[0]: expr=expr[1:-1]
            expr=re.sub(r'F\.col\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',r'"\2"',expr).replace("==","=")
            ops.append(Operation(op="filter",args={"condition":expr})); continue
        if ".select(" in line:
            cols=re.findall(r'["\']([A-Za-z_][A-Za-z0-9_]*)["\']',line)
            if not cols: raise ValueError("select() expects quoted column names")
            ops.append(Operation(op="select",args={"columns":cols})); continue
        if ".withColumn(" in line:
            m=re.search(r'\.withColumn\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\s*,\s*(.+)\)\s*$',line)
            if not m: raise ValueError(f"Could not parse withColumn: {line}")
            expr=re.sub(r'F\.col\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',r'"\2"',m.group(3))
            ops.append(Operation(op="with_column",args={"name":m.group(2),"expression":expr})); continue
        if ".groupBy(" in line and ".agg(" in line:
            m=re.search(r'\.groupBy\((.*?)\)\.agg\((.*)\)\s*$',line)
            keys=re.findall(r'["\']([A-Za-z_][A-Za-z0-9_]*)["\']',m.group(1)) if m else []
            agg=re.fullmatch(r'F\.(sum|avg|mean|min|max|count)\((["\'])([A-Za-z_][A-Za-z0-9_]*)\2\)\.alias\((["\'])([A-Za-z_][A-Za-z0-9_]*)\5\)',m.group(2).strip()) if m else None
            if not agg: raise ValueError("This first compiler version supports one aliased aggregation per groupBy")
            fn="avg" if agg.group(1)=="mean" else agg.group(1)
            ops.append(Operation(op="group_by",args={"keys":keys,"aggregations":[{"function":fn,"column":agg.group(3),"alias":agg.group(6)}]})); continue
        if ".orderBy(" in line or ".sort(" in line:
            d=re.search(r'F\.desc\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
            a=re.search(r'F\.asc\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
            p=re.search(r'\.(?:orderBy|sort)\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
            if d: item={"column":d.group(2),"direction":"desc"}
            elif a: item={"column":a.group(2),"direction":"asc"}
            elif p: item={"column":p.group(2),"direction":"asc"}
            else: raise ValueError("Unsupported order expression")
            ops.append(Operation(op="order_by",args={"columns":[item]})); continue
        for method,key in (("limit","count"),("repartition","partitions")):
            if f".{method}(" in line:
                m=re.search(rf'\.{method}\((\d+)\)\s*$',line)
                if not m: raise ValueError(f"Could not parse {method}")
                ops.append(Operation(op=method if method=="repartition" else "limit",args={key:int(m.group(1))})); break
        else:
            if ".distinct()" in line: ops.append(Operation(op="distinct")); continue
            if ".dropDuplicates(" in line:
                keys=re.findall(r'["\']([A-Za-z_][A-Za-z0-9_]*)["\']',line)
                ops.append(Operation(op="drop_duplicates",args={"keys":keys})); continue
            if any(x in line for x in (".show(", ".collect(", ".count()", ".printSchema(")):
                warnings.append(f"Action ignored by compiler: {line}"); continue
            if line.startswith(("from pyspark","import pyspark")): continue
            raise ValueError(f"Unsupported PySpark statement: {line}")
    if source is None: raise ValueError('Start with a source such as: df = spark.table("sales")')
    return source,ops,warnings

real_spark = RealSparkOracle()

app=FastAPI(
    title="Datapass Fake Spark Runtime",
    version="0.1.0",
    description="DuckDB executes bounded local data while a deterministic model simulates Spark stages, shuffles, partitions, spill, skew and fictional Datapass compute credits."
)
origins=[x.strip() for x in os.getenv("DATAPASS_CORS_ORIGINS","*").split(",") if x.strip()]
app.add_middleware(CORSMiddleware,allow_origins=origins or ["*"],allow_credentials=False,allow_methods=["GET","POST"],allow_headers=["Content-Type","Authorization"])

@app.get("/")
def root():
    return {"service":"datapass-fake-spark","version":app.version,"status":"ok","docs":"/docs","mode":"duckdb-execution + deterministic-spark-simulation"}

@app.get("/health")
def health():
    return {"status":"ok","service":"datapass-fake-spark"}

@app.get("/v1/runtimes")
def runtimes():
    return {"items":[{"id":k,**v,"note":"Fictional Datapass teaching profile; not vendor pricing."} for k,v in RUNTIMES.items()]}

@app.post("/v1/spark/compile")
def compile_code(req: CompileRequest):
    try:
        source,ops,warnings=compile_pyspark(req.code)
        return {"source_table":source,"operations":[o.model_dump(mode="json") for o in ops],"warnings":warnings}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e

@app.post("/v1/spark/plan")
def plan(req: PlanRequest):
    try:
        return {"sql":compile_plan_sql(req.source_table,req.operations),"metrics":simulate(req.runtime_id,req.source_table,req.operations,req.tables,req.hints)}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e

@app.post("/v1/spark/execute")
def execute(req: ExecuteRequest):
    try:
        sql=compile_plan_sql(req.source_table,req.operations)
        cols,rows,truncated=execute_sql(req.tables,sql,req.collect_limit)
        return {"execution_id":"dps_"+uuid.uuid4().hex[:16],"columns":cols,"rows":rows,"truncated":truncated,"physical_engine":"duckdb","metrics":simulate(req.runtime_id,req.source_table,req.operations,req.tables,req.hints)}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e
    except Exception as e:
        raise HTTPException(400,f"Execution failed: {e}") from e

@app.get("/v1/spark/verify/capabilities")
def verify_capabilities():
    return real_spark.capabilities()

@app.post("/v1/spark/verify", status_code=202)
def verify(req: VerifyRequest, _: None = Depends(require_runner_key)):
    try:
        return real_spark.dispatch(
            code=req.code,
            tables=[table.model_dump(mode="json") for table in req.tables],
            collect_limit=req.collect_limit,
        )
    except ValueError as e:
        raise HTTPException(422,str(e)) from e
    except RuntimeError as e:
        raise HTTPException(503,str(e)) from e

@app.get("/v1/spark/verify/{run_id}")
def verify_status(run_id: int, _: None = Depends(require_runner_key)):
    try:
        return real_spark.status(run_id)
    except ValueError as e:
        raise HTTPException(404,str(e)) from e
    except RuntimeError as e:
        raise HTTPException(503,str(e)) from e

@app.get("/v1/spark/verify/{run_id}/result/{request_id}")
def verify_result(run_id: int, request_id: str, _: None = Depends(require_runner_key)):
    try:
        return real_spark.result(run_id, request_id)
    except ValueError as e:
        raise HTTPException(404,str(e)) from e
    except RuntimeError as e:
        raise HTTPException(503,str(e)) from e

@app.post("/v1/spark/sql")
def spark_sql(req: SqlRequest):
    try:
        cols,rows,truncated=execute_sql(req.tables,req.sql,req.collect_limit)
        source=req.tables[0].name if req.tables else "input"
        return {"execution_id":"dps_"+uuid.uuid4().hex[:16],"columns":cols,"rows":rows,"truncated":truncated,"physical_engine":"duckdb","metrics":simulate(req.runtime_id,source,[],req.tables,req.hints)}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e
    except Exception as e:
        raise HTTPException(400,f"Execution failed: {e}") from e
)
    rows: list[dict[str, Any]] = Field(default_factory=list, max_length=5000)

    @field_validator("rows")
    @classmethod
    def validate_rows(cls, rows):
        if any(len(row) > 100 for row in rows):
            raise ValueError("Each row may contain at most 100 columns")
        return rows


class VerifyRequest(BaseModel):
    code: str = Field(min_length=1, max_length=20_000)
    tables: list[VerifyTableData] = Field(default_factory=list, max_length=8)
    collect_limit: int = Field(default=100, ge=1, le=200)

    @model_validator(mode="after")
    def bounded_fixture(self):
        if sum(len(table.rows) for table in self.tables) > 5000:
            raise ValueError("Real Spark verification allows at most 5000 fixture rows total")
        return self


def require_runner_key(x_datapass_runner_key: str | None = Header(default=None)):
    expected = os.getenv("DATAPASS_RUNNER_KEY")
    if not expected:
        raise HTTPException(503, "Real Spark verification is disabled until DATAPASS_RUNNER_KEY is configured.")
    if not x_datapass_runner_key or not hmac.compare_digest(x_datapass_runner_key, expected):
        raise HTTPException(401, "Invalid Datapass runner key.")


RUNTIMES = {
    "datapass-free": dict(label="Datapass Free Lab", executors=1, cores=2, memory_gb=1.0, partitions=4, scan=180, shuffle=90, rows_sec=180000, startup=80, credits_hour=0.0),
    "datapass-s": dict(label="Datapass S", executors=2, cores=2, memory_gb=2.0, partitions=8, scan=420, shuffle=190, rows_sec=240000, startup=120, credits_hour=0.06),
    "datapass-m": dict(label="Datapass M", executors=4, cores=4, memory_gb=4.0, partitions=32, scan=1050, shuffle=520, rows_sec=310000, startup=180, credits_hour=0.18),
    "datapass-l": dict(label="Datapass L", executors=8, cores=4, memory_gb=8.0, partitions=64, scan=2100, shuffle=1100, rows_sec=360000, startup=260, credits_hour=0.42),
}

def qident(value: str) -> str:
    if not IDENT.fullmatch(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return f'"{value}"'

def validate_expr(text: str) -> str:
    text = text.strip()
    if not text or ";" in text or "--" in text or "/*" in text or "*/" in text or FORBIDDEN_SQL.search(text):
        raise ValueError("Unsafe or empty expression")
    return text

def validate_query(text: str) -> str:
    text = text.strip().rstrip(";").strip()
    if not re.match(r"^(select|with)\b", text, flags=re.I):
        raise ValueError("Only SELECT or WITH queries are allowed")
    if ";" in text or "--" in text or "/*" in text or "*/" in text or FORBIDDEN_SQL.search(text):
        raise ValueError("Only one safe in-memory query is allowed")
    return text

def compile_plan_sql(source: str, operations: list[Operation]) -> str:
    sql = f"SELECT * FROM {qident(source)}"
    for i, op in enumerate(operations):
        a = f"q{i}"
        if op.op == OperationType.FILTER:
            sql = f"SELECT * FROM ({sql}) AS {a} WHERE {validate_expr(str(op.args.get('condition','')))}"
        elif op.op == OperationType.SELECT:
            cols = op.args.get("columns") or []
            if not cols: raise ValueError("select requires columns")
            sql = f"SELECT {', '.join(qident(str(c)) for c in cols)} FROM ({sql}) AS {a}"
        elif op.op == OperationType.WITH_COLUMN:
            name = qident(str(op.args.get("name","")))
            expr = validate_expr(str(op.args.get("expression","")))
            sql = f"SELECT *, {expr} AS {name} FROM ({sql}) AS {a}"
        elif op.op == OperationType.GROUP_BY:
            keys = [qident(str(k)) for k in op.args.get("keys",[])]
            aggs = []
            for item in op.args.get("aggregations") or []:
                fn = str(item.get("function","")).lower()
                if fn not in {"sum","avg","min","max","count"}: raise ValueError("Unsupported aggregation")
                aggs.append(f"{fn.upper()}({qident(str(item['column']))}) AS {qident(str(item['alias']))}")
            if not aggs: raise ValueError("group_by requires aggregations")
            sql = f"SELECT {', '.join(keys+aggs)} FROM ({sql}) AS {a}" + (f" GROUP BY {', '.join(keys)}" if keys else "")
        elif op.op == OperationType.ORDER_BY:
            terms=[]
            for item in op.args.get("columns") or []:
                d=str(item.get("direction","asc")).lower()
                if d not in {"asc","desc"}: raise ValueError("direction must be asc or desc")
                terms.append(f"{qident(str(item['column']))} {d.upper()}")
            if not terms: raise ValueError("order_by requires columns")
            sql=f"SELECT * FROM ({sql}) AS {a} ORDER BY {', '.join(terms)}"
        elif op.op == OperationType.LIMIT:
            n=int(op.args.get("count",0))
            if n < 0 or n > 1_000_000: raise ValueError("limit out of range")
            sql=f"SELECT * FROM ({sql}) AS {a} LIMIT {n}"
        elif op.op == OperationType.DISTINCT:
            sql=f"SELECT DISTINCT * FROM ({sql}) AS {a}"
        elif op.op == OperationType.DROP_DUPLICATES:
            keys=op.args.get("keys") or []
            if not keys: sql=f"SELECT DISTINCT * FROM ({sql}) AS {a}"
            else:
                qq=", ".join(qident(str(k)) for k in keys)
                sql=f"SELECT * EXCLUDE (__rn) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {qq}) AS __rn FROM ({sql}) AS {a}) WHERE __rn=1"
        elif op.op == OperationType.REPARTITION:
            pass
        elif op.op == OperationType.JOIN:
            right=qident(str(op.args.get("right_table","")))
            keys=op.args.get("on") or []
            how=str(op.args.get("how","inner")).lower()
            jmap={"inner":"INNER","left":"LEFT","right":"RIGHT","full":"FULL"}
            if not keys or how not in jmap: raise ValueError("Invalid join")
            la,ra=f"l{i}",f"r{i}"
            pred=" AND ".join(f"{la}.{qident(str(k))}={ra}.{qident(str(k))}" for k in keys)
            sql=f"SELECT {la}.*, {ra}.* EXCLUDE ({', '.join(qident(str(k)) for k in keys)}) FROM ({sql}) AS {la} {jmap[how]} JOIN {right} AS {ra} ON {pred}"
    return sql

def register_table(con, table: TableData):
    cols=[]
    seen=set()
    for row in table.rows:
        for k in row:
            qident(k)
            if k not in seen: seen.add(k); cols.append(k)
    if not cols:
        con.execute(f"CREATE TABLE {qident(table.name)} (__empty INTEGER)")
        return
    def dtype(values):
        vals=[v for v in values if v is not None]
        if not vals: return "VARCHAR"
        if all(isinstance(v,bool) for v in vals): return "BOOLEAN"
        if all(isinstance(v,int) and not isinstance(v,bool) for v in vals): return "BIGINT"
        if all(isinstance(v,(int,float)) and not isinstance(v,bool) for v in vals): return "DOUBLE"
        return "VARCHAR"
    defs=", ".join(f"{qident(c)} {dtype([r.get(c) for r in table.rows])}" for c in cols)
    con.execute(f"CREATE TABLE {qident(table.name)} ({defs})")
    if table.rows:
        con.executemany(
            f"INSERT INTO {qident(table.name)} VALUES ({', '.join(['?']*len(cols))})",
            [[r.get(c) if r.get(c) is None or isinstance(r.get(c),(str,int,float,bool)) else str(r.get(c)) for c in cols] for r in table.rows],
        )

def execute_sql(tables: list[TableData], sql: str, limit: int):
    con=duckdb.connect(":memory:")
    for stmt in ("SET enable_external_access=false","SET autoinstall_known_extensions=false","SET autoload_known_extensions=false"):
        try: con.execute(stmt)
        except Exception: pass
    for t in tables: register_table(con,t)
    try:
        cur=con.execute(f"SELECT * FROM ({validate_query(sql)}) AS datapass_result LIMIT {limit+1}")
        cols=[d[0] for d in cur.description]
        raw=cur.fetchall()
    finally:
        con.close()
    return cols,[dict(zip(cols,row,strict=True)) for row in raw[:limit]],len(raw)>limit

def simulate(runtime_id: str, source: str, ops: list[Operation], tables: list[TableData], hints: SimulationHints):
    if runtime_id not in RUNTIMES: raise ValueError("Unknown runtime_id")
    rt=RUNTIMES[runtime_id]
    table=next((t for t in tables if t.name==source),None)
    obs_rows=len(table.rows) if table else 100000
    obs_bytes=max(sum(len(json.dumps(r,default=str).encode()) for r in table.rows),obs_rows*16) if table else 12_000_000
    rows=hints.input_rows if hints.input_rows is not None else obs_rows
    size=hints.input_bytes if hints.input_bytes is not None else obs_bytes
    parts=hints.partitions or rt["partitions"]
    stages=[]; warnings=[]; current_ops=[]; pending=0
    stage_rows=rows; stage_bytes=size
    wide={OperationType.GROUP_BY,OperationType.ORDER_BY,OperationType.DISTINCT,OperationType.DROP_DUPLICATES,OperationType.REPARTITION,OperationType.JOIN}
    def finish(reason, sw=0):
        nonlocal current_ops,pending,stage_rows,stage_bytes
        if not current_ops and not stages: current_ops=["scan"]
        cores=rt["executors"]*rt["cores"]
        duration=max(1,int((rt["startup"]/1000 + stage_bytes/(rt["scan"]*1024*1024) + (pending+sw)/(rt["shuffle"]*1024*1024) + stage_rows/(rt["rows_sec"]*cores) + parts*0.0025/cores)*1000*(1+(hints.skew_factor-1)*0.22)))
        budget=rt["executors"]*rt["memory_gb"]*1024**3*0.62
        spill=max(0,int((stage_bytes+pending+sw-budget)*0.55))
        stages.append(dict(stage_id=len(stages),reason=reason,operations=list(current_ops),tasks=max(1,parts),input_rows=max(0,stage_rows),output_rows=max(0,rows),input_bytes=max(0,stage_bytes),shuffle_read_bytes=max(0,pending),shuffle_write_bytes=max(0,sw),spill_bytes=spill,duration_ms=duration,skew_factor=hints.skew_factor))
        if spill: warnings.append("Simulated spill detected: executor memory is undersized for this stage.")
        current_ops=[]; pending=sw; stage_rows=rows; stage_bytes=size
    for op in ops:
        current_ops.append(op.op.value)
        if op.op==OperationType.FILTER: rows=int(rows*hints.filter_selectivity); size=int(size*hints.filter_selectivity)
        elif op.op==OperationType.SELECT: size=int(size*min(1,max(.12,len(op.args.get("columns",[]))/10 or .5)))
        elif op.op==OperationType.WITH_COLUMN: size=int(size*1.08)
        elif op.op==OperationType.LIMIT:
            n=int(op.args.get("count",rows)); ratio=min(1,n/max(rows,1)); rows=min(rows,n); size=int(size*ratio)
        elif op.op==OperationType.GROUP_BY: rows=max(1,int(rows*hints.group_cardinality_ratio)) if rows else 0; size=max(rows*48,int(size*.18))
        elif op.op in {OperationType.DISTINCT,OperationType.DROP_DUPLICATES}: rows=int(rows*.7); size=int(size*.72)
        elif op.op==OperationType.JOIN: rows=int(rows*1.12); size=int(size*1.25)
        elif op.op==OperationType.REPARTITION: parts=max(1,int(op.args.get("partitions",parts)))
        if op.op in wide:
            ratios={OperationType.GROUP_BY:.68,OperationType.ORDER_BY:1,OperationType.DISTINCT:.82,OperationType.DROP_DUPLICATES:.82,OperationType.REPARTITION:1,OperationType.JOIN:.92}
            sw=int(size*(.08 if op.op==OperationType.JOIN and op.args.get("broadcast") else ratios[op.op]))
            finish(f"shuffle boundary: {op.op.value}",sw)
            if op.op==OperationType.GROUP_BY: warnings.append("groupBy creates a wide dependency; inspect key cardinality and skew.")
            if op.op==OperationType.ORDER_BY: warnings.append("Global orderBy/sort requires a shuffle.")
            if op.op==OperationType.REPARTITION: warnings.append("repartition() forces a full shuffle.")
    if current_ops or not stages: finish("result stage")
    total_ms=sum(s["duration_ms"] for s in stages)
    if hints.skew_factor>=2: warnings.append("High simulated skew factor: a minority of partitions dominate runtime.")
    return dict(
        runtime_id=runtime_id,total_duration_ms=total_ms,total_tasks=sum(s["tasks"] for s in stages),
        total_shuffle_bytes=sum(s["shuffle_read_bytes"]+s["shuffle_write_bytes"] for s in stages),
        total_spill_bytes=sum(s["spill_bytes"] for s in stages),
        simulated_credits=round((total_ms/3_600_000)*rt["credits_hour"],6),
        stages=stages,warnings=list(dict.fromkeys(warnings)),
        disclaimer="Distributed metrics and Datapass credits are simulations for learning; they are not measurements or vendor pricing."
    )

def compile_pyspark(code: str):
    source=None; ops=[]; warnings=[]
    lines=[x.strip() for x in code.splitlines() if x.strip() and not x.strip().startswith("#")]
    for line in lines:
        m=re.search(r'spark\.table\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
        if m: source=m.group(2); continue
        if ".filter(" in line or ".where(" in line:
            m=re.search(r'\.(?:filter|where)\((.+)\)\s*$',line)
            if not m: raise ValueError(f"Could not parse filter: {line}")
            expr=m.group(1).strip()
            if len(expr)>=2 and expr[0] in {'"',"'"} and expr[-1]==expr[0]: expr=expr[1:-1]
            expr=re.sub(r'F\.col\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',r'"\2"',expr).replace("==","=")
            ops.append(Operation(op="filter",args={"condition":expr})); continue
        if ".select(" in line:
            cols=re.findall(r'["\']([A-Za-z_][A-Za-z0-9_]*)["\']',line)
            if not cols: raise ValueError("select() expects quoted column names")
            ops.append(Operation(op="select",args={"columns":cols})); continue
        if ".withColumn(" in line:
            m=re.search(r'\.withColumn\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\s*,\s*(.+)\)\s*$',line)
            if not m: raise ValueError(f"Could not parse withColumn: {line}")
            expr=re.sub(r'F\.col\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',r'"\2"',m.group(3))
            ops.append(Operation(op="with_column",args={"name":m.group(2),"expression":expr})); continue
        if ".groupBy(" in line and ".agg(" in line:
            m=re.search(r'\.groupBy\((.*?)\)\.agg\((.*)\)\s*$',line)
            keys=re.findall(r'["\']([A-Za-z_][A-Za-z0-9_]*)["\']',m.group(1)) if m else []
            agg=re.fullmatch(r'F\.(sum|avg|mean|min|max|count)\((["\'])([A-Za-z_][A-Za-z0-9_]*)\2\)\.alias\((["\'])([A-Za-z_][A-Za-z0-9_]*)\5\)',m.group(2).strip()) if m else None
            if not agg: raise ValueError("This first compiler version supports one aliased aggregation per groupBy")
            fn="avg" if agg.group(1)=="mean" else agg.group(1)
            ops.append(Operation(op="group_by",args={"keys":keys,"aggregations":[{"function":fn,"column":agg.group(3),"alias":agg.group(6)}]})); continue
        if ".orderBy(" in line or ".sort(" in line:
            d=re.search(r'F\.desc\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
            a=re.search(r'F\.asc\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
            p=re.search(r'\.(?:orderBy|sort)\((["\'])([A-Za-z_][A-Za-z0-9_]*)\1\)',line)
            if d: item={"column":d.group(2),"direction":"desc"}
            elif a: item={"column":a.group(2),"direction":"asc"}
            elif p: item={"column":p.group(2),"direction":"asc"}
            else: raise ValueError("Unsupported order expression")
            ops.append(Operation(op="order_by",args={"columns":[item]})); continue
        for method,key in (("limit","count"),("repartition","partitions")):
            if f".{method}(" in line:
                m=re.search(rf'\.{method}\((\d+)\)\s*$',line)
                if not m: raise ValueError(f"Could not parse {method}")
                ops.append(Operation(op=method if method=="repartition" else "limit",args={key:int(m.group(1))})); break
        else:
            if ".distinct()" in line: ops.append(Operation(op="distinct")); continue
            if ".dropDuplicates(" in line:
                keys=re.findall(r'["\']([A-Za-z_][A-Za-z0-9_]*)["\']',line)
                ops.append(Operation(op="drop_duplicates",args={"keys":keys})); continue
            if any(x in line for x in (".show(", ".collect(", ".count()", ".printSchema(")):
                warnings.append(f"Action ignored by compiler: {line}"); continue
            if line.startswith(("from pyspark","import pyspark")): continue
            raise ValueError(f"Unsupported PySpark statement: {line}")
    if source is None: raise ValueError('Start with a source such as: df = spark.table("sales")')
    return source,ops,warnings

real_spark = RealSparkOracle()

app=FastAPI(
    title="Datapass Fake Spark Runtime",
    version="0.1.0",
    description="DuckDB executes bounded local data while a deterministic model simulates Spark stages, shuffles, partitions, spill, skew and fictional Datapass compute credits."
)
origins=[x.strip() for x in os.getenv("DATAPASS_CORS_ORIGINS","*").split(",") if x.strip()]
app.add_middleware(CORSMiddleware,allow_origins=origins or ["*"],allow_credentials=False,allow_methods=["GET","POST"],allow_headers=["Content-Type","Authorization"])

@app.get("/")
def root():
    return {"service":"datapass-fake-spark","version":app.version,"status":"ok","docs":"/docs","mode":"duckdb-execution + deterministic-spark-simulation"}

@app.get("/health")
def health():
    return {"status":"ok","service":"datapass-fake-spark"}

@app.get("/v1/runtimes")
def runtimes():
    return {"items":[{"id":k,**v,"note":"Fictional Datapass teaching profile; not vendor pricing."} for k,v in RUNTIMES.items()]}

@app.post("/v1/spark/compile")
def compile_code(req: CompileRequest):
    try:
        source,ops,warnings=compile_pyspark(req.code)
        return {"source_table":source,"operations":[o.model_dump(mode="json") for o in ops],"warnings":warnings}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e

@app.post("/v1/spark/plan")
def plan(req: PlanRequest):
    try:
        return {"sql":compile_plan_sql(req.source_table,req.operations),"metrics":simulate(req.runtime_id,req.source_table,req.operations,req.tables,req.hints)}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e

@app.post("/v1/spark/execute")
def execute(req: ExecuteRequest):
    try:
        sql=compile_plan_sql(req.source_table,req.operations)
        cols,rows,truncated=execute_sql(req.tables,sql,req.collect_limit)
        return {"execution_id":"dps_"+uuid.uuid4().hex[:16],"columns":cols,"rows":rows,"truncated":truncated,"physical_engine":"duckdb","metrics":simulate(req.runtime_id,req.source_table,req.operations,req.tables,req.hints)}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e
    except Exception as e:
        raise HTTPException(400,f"Execution failed: {e}") from e

@app.get("/v1/spark/verify/capabilities")
def verify_capabilities():
    return real_spark.capabilities()

@app.post("/v1/spark/verify", status_code=202)
def verify(req: VerifyRequest, _: None = Depends(require_runner_key)):
    try:
        return real_spark.dispatch(
            code=req.code,
            tables=[table.model_dump(mode="json") for table in req.tables],
            collect_limit=req.collect_limit,
        )
    except ValueError as e:
        raise HTTPException(422,str(e)) from e
    except RuntimeError as e:
        raise HTTPException(503,str(e)) from e

@app.get("/v1/spark/verify/{run_id}")
def verify_status(run_id: int, _: None = Depends(require_runner_key)):
    try:
        return real_spark.status(run_id)
    except ValueError as e:
        raise HTTPException(404,str(e)) from e
    except RuntimeError as e:
        raise HTTPException(503,str(e)) from e

@app.get("/v1/spark/verify/{run_id}/result/{request_id}")
def verify_result(run_id: int, request_id: str, _: None = Depends(require_runner_key)):
    try:
        return real_spark.result(run_id, request_id)
    except ValueError as e:
        raise HTTPException(404,str(e)) from e
    except RuntimeError as e:
        raise HTTPException(503,str(e)) from e

@app.post("/v1/spark/sql")
def spark_sql(req: SqlRequest):
    try:
        cols,rows,truncated=execute_sql(req.tables,req.sql,req.collect_limit)
        source=req.tables[0].name if req.tables else "input"
        return {"execution_id":"dps_"+uuid.uuid4().hex[:16],"columns":cols,"rows":rows,"truncated":truncated,"physical_engine":"duckdb","metrics":simulate(req.runtime_id,source,[],req.tables,req.hints)}
    except ValueError as e:
        raise HTTPException(422,str(e)) from e
    except Exception as e:
        raise HTTPException(400,f"Execution failed: {e}") from e
