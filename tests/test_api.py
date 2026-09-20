from fastapi.testclient import TestClient
from main import app, compile_pyspark, Operation, SimulationHints, simulate

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
