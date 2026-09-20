from fastapi.testclient import TestClient
from main import app, compile_pyspark, Operation, SimulationHints, VerifyRequest, simulate

client = TestClient(app)

def test_health():
    r=client.get("/health")
    assert r.status_code==200 and r.json()["status"]=="ok"

def test_compile_rejects_arbitrary_python():
    r=client.post("/v1/spark/compile",json={"code":'df = spark.table("sales")\nimport os\nos.system("whoami")'})
    assert r.status_code==422

def test_compile_common_subset():
    source,ops,warnings=compile_pyspark('df = spark.table("sales")\ndf = df.filter(F.col("amount") > 100)\ndf = df.limit(10)')
    assert source=="sales" and [o.op.value for o in ops]==["filter","limit"] and not warnings

def test_wide_stage_shuffle():
    m=simulate("datapass-s","sales",[Operation(op="group_by",args={"keys":["region"],"aggregations":[{"function":"sum","column":"amount","alias":"revenue"}]})],[],SimulationHints(input_rows=5_000_000,input_bytes=900_000_000,partitions=32,skew_factor=1.5))
    assert m["total_shuffle_bytes"]>0 and m["stages"]


def test_real_spark_capability_is_explicit_when_unconfigured():
    r=client.get("/v1/spark/verify/capabilities")
    assert r.status_code==200
    body=r.json()
    assert body["mode"]=="github_actions_ephemeral"
    assert body["spark_version"]=="4.2.0"
    assert body["master"]=="local[4]"
    assert body["cluster_truth"].startswith("single-host Spark")


def test_real_spark_verify_requires_server_key(monkeypatch):
    monkeypatch.delenv("DATAPASS_RUNNER_KEY", raising=False)
    payload={"code":'result = spark.table("sales")',"tables":[{"name":"sales","rows":[{"id":1}]}],"collect_limit":10}
    r=client.post("/v1/spark/verify",json=payload)
    assert r.status_code==503

    monkeypatch.setenv("DATAPASS_RUNNER_KEY","server-secret")
    r=client.post("/v1/spark/verify",json=payload)
    assert r.status_code==401
    r=client.post("/v1/spark/verify",json=payload,headers={"X-Datapass-Runner-Key":"server-secret"})
    assert r.status_code==503  # GitHub token is intentionally not configured in unit tests.


def test_real_spark_verify_accepts_one_level_schema_table_names():
    req=VerifyRequest(
        code='result = spark.table("silver.orders")',
        tables=[{"name":"silver.orders","rows":[{"order_id":"O-1","amount":10}]}],
        collect_limit=10,
    )
    assert req.tables[0].name=="silver.orders"
