"""Real Spark oracle executed on ephemeral GitHub Actions runners.

The normal Datapass runtime stays fast: DuckDB returns bounded results and a
separate model simulates distributed Spark behavior. This adapter is the
optional verification path that runs genuine Apache Spark in local[4] mode on
one GitHub-hosted VM.

The GitHub token is server-only. Submitted code is arbitrary learner code and
must be treated as untrusted educational material; the workflow deliberately
does not check out the repository or expose repository secrets.
"""
from __future__ import annotations

from dataclasses import dataclass
import base64
import io
import json
import os
import re
import secrets
import zipfile
from typing import Any

import httpx


API_VERSION = "2026-03-10"
SPARK_VERSION = "4.2.0"
REQUEST_ID = re.compile(r"^[a-f0-9]{16}$")
MAX_CODE_BYTES = 20_000
MAX_TABLES_BYTES = 20_000
MAX_LOG_CHARS = 30_000


@dataclass(frozen=True)
class RealSparkConfig:
    repo: str
    workflow: str
    ref: str
    token: str | None
    public_repo: bool

    @classmethod
    def from_env(cls) -> "RealSparkConfig":
        return cls(
            repo=os.environ.get("DATAPASS_SPARK_GITHUB_REPO", "julian-passebecq/fastapispark"),
            workflow=os.environ.get("DATAPASS_SPARK_GITHUB_WORKFLOW", "real-spark.yml"),
            ref=os.environ.get("DATAPASS_SPARK_GITHUB_REF", "main"),
            token=os.environ.get("DATAPASS_GITHUB_TOKEN") or None,
            public_repo=os.environ.get("DATAPASS_SPARK_PUBLIC_REPO", "1") == "1",
        )

    @property
    def configured(self) -> bool:
        return bool(self.token and "/" in self.repo and self.workflow and self.ref)


class RealSparkOracle:
    def __init__(self, config: RealSparkConfig | None = None, *, timeout: float = 20.0):
        self.config = config or RealSparkConfig.from_env()
        self.timeout = timeout

    def capabilities(self) -> dict[str, Any]:
        c = self.config
        return {
            "schema_version": 1,
            "enabled": c.configured,
            "mode": "github_actions_ephemeral",
            "spark_version": SPARK_VERSION,
            "master": "local[4]",
            "repo": c.repo,
            "workflow": c.workflow,
            "ref": c.ref,
            "public_repo": c.public_repo,
            "truth": (
                "real Apache Spark 4.2.0 execution on one ephemeral GitHub-hosted VM"
                if c.configured
                else "unavailable until the server configures a GitHub Actions write token"
            ),
            "cluster_truth": "single-host Spark local[4]; not a multi-machine Spark cluster",
            "privacy": (
                "Public execution repository: submitted code, logs and artifacts must contain no secrets."
                if c.public_repo
                else "Execution repository is private; still do not embed credentials in learner code."
            ),
            "limits": {
                "code_bytes": MAX_CODE_BYTES,
                "tables_json_bytes": MAX_TABLES_BYTES,
                "job_timeout_minutes": 15,
                "execution_timeout_seconds": 360,
            },
        }

    def _headers(self) -> dict[str, str]:
        if not self.config.token:
            raise RuntimeError("Real Spark is not configured. Set DATAPASS_GITHUB_TOKEN on the FastAPI server.")
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.config.token}",
            "X-GitHub-Api-Version": API_VERSION,
        }

    def _url(self, suffix: str) -> str:
        return f"https://api.github.com/repos/{self.config.repo}{suffix}"

    def dispatch(self, *, code: str, tables: list[dict[str, Any]], collect_limit: int) -> dict[str, Any]:
        if not self.config.configured:
            raise RuntimeError("Real Spark verification is unavailable until GitHub Actions is configured.")
        raw_code = code.encode("utf-8")
        raw_tables = json.dumps(tables, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if not raw_code or len(raw_code) > MAX_CODE_BYTES:
            raise ValueError(f"Spark verification code must be 1..{MAX_CODE_BYTES} UTF-8 bytes.")
        if len(raw_tables) > MAX_TABLES_BYTES:
            raise ValueError(f"Spark verification tables JSON exceeds {MAX_TABLES_BYTES} bytes.")
        if collect_limit < 1 or collect_limit > 200:
            raise ValueError("collect_limit must be between 1 and 200.")
        if "\x00" in code:
            raise ValueError("Spark verification code may not contain NUL bytes.")

        request_id = secrets.token_hex(8)
        payload = {
            "ref": self.config.ref,
            "return_run_details": True,
            "inputs": {
                "request_id": request_id,
                "code_b64": base64.b64encode(raw_code).decode("ascii"),
                "tables_b64": base64.b64encode(raw_tables).decode("ascii"),
                "collect_limit": str(collect_limit),
            },
        }
        url = self._url(f"/actions/workflows/{self.config.workflow}/dispatches")
        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            response = client.post(url, headers=self._headers(), json=payload)
        if response.status_code != 200:
            raise RuntimeError(
                f"GitHub Spark workflow dispatch failed ({response.status_code}): {response.text[:1000]}"
            )
        body = response.json()
        run_id = body.get("workflow_run_id")
        if not run_id:
            raise RuntimeError("GitHub accepted the Spark dispatch but returned no workflow run id.")
        return {
            "request_id": request_id,
            "status": "accepted",
            "run_id": int(run_id),
            "run_url": body.get("html_url"),
            "truth": "GitHub Actions accepted a real Spark verification job; Spark has not completed yet",
        }

    def _run(self, run_id: int) -> dict[str, Any]:
        if run_id <= 0:
            raise ValueError("Invalid GitHub Actions run id.")
        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            response = client.get(self._url(f"/actions/runs/{run_id}"), headers=self._headers())
        if response.status_code == 404:
            raise ValueError("Real Spark workflow run not found.")
        if response.status_code != 200:
            raise RuntimeError(f"GitHub Spark workflow status lookup failed ({response.status_code}).")
        return response.json()

    def status(self, run_id: int) -> dict[str, Any]:
        run = self._run(run_id)
        return {
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "run_id": run.get("id"),
            "run_url": run.get("html_url"),
            "created_at": run.get("created_at"),
            "run_started_at": run.get("run_started_at"),
            "updated_at": run.get("updated_at"),
            "artifact_available": run.get("status") == "completed",
            "truth": "real GitHub Actions workflow state",
        }

    def result(self, run_id: int, request_id: str) -> dict[str, Any]:
        if not REQUEST_ID.fullmatch(request_id):
            raise ValueError("Invalid Spark verification request id.")
        run = self._run(run_id)
        if run.get("status") != "completed":
            raise RuntimeError("The real Spark workflow has not completed yet.")

        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            artifacts = client.get(
                self._url(f"/actions/runs/{run_id}/artifacts"),
                headers=self._headers(),
            )
            if artifacts.status_code != 200:
                raise RuntimeError(f"Could not list Spark artifacts ({artifacts.status_code}).")
            expected = f"datapass-real-spark-{request_id}"
            artifact = next(
                (item for item in artifacts.json().get("artifacts", []) if item.get("name") == expected),
                None,
            )
            if artifact is None:
                raise RuntimeError("The real Spark result artifact is not available.")
            archive = client.get(artifact["archive_download_url"], headers=self._headers())
            if archive.status_code != 200:
                raise RuntimeError(f"Could not download Spark artifact ({archive.status_code}).")

        with zipfile.ZipFile(io.BytesIO(archive.content)) as bundle:
            try:
                result = json.loads(bundle.read("result.json"))
            except (KeyError, json.JSONDecodeError) as error:
                raise RuntimeError("Spark artifact is missing a valid result.json.") from error

            def text_file(name: str) -> str:
                try:
                    return bundle.read(name).decode("utf-8", "replace")
                except KeyError:
                    return ""

            stdout = text_file("spark-run.log")
            result["logical_plan"] = text_file("logical-plan.txt")[-MAX_LOG_CHARS:]
            result["physical_plan"] = text_file("physical-plan.txt")[-MAX_LOG_CHARS:]
            result["formatted_plan"] = text_file("formatted-plan.txt")[-MAX_LOG_CHARS:]
            result["log"] = stdout[-MAX_LOG_CHARS:]
            result["log_truncated"] = len(stdout) > MAX_LOG_CHARS

        result["request_id"] = request_id
        result["run_id"] = run_id
        result["run_url"] = run.get("html_url")
        result["truth"] = (
            "real Apache Spark 4.2.0 local[4] execution on one ephemeral GitHub-hosted VM"
        )
        return result
