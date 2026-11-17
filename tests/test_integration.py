import json

import pytest

from boxoffice.domain.workload import WorkloadSpec
from boxoffice.launcher import LocalDeployment, run_local
from mpcomm.transport.faults import FaultProfile

pytestmark = pytest.mark.integration


def test_local_deployment_delivers_every_command_to_every_replica(tmp_path):
    report_path = tmp_path / "report.json"
    deployment = LocalDeployment(
        offices=4,
        sessions=2,
        workload=WorkloadSpec(commands=25, seed=1),
        base_port=7600,
        faults=FaultProfile(loss=0.1, delay_ms=15, jitter_ms=15, seed=5),
        report=report_path,
        timeout=300,
    )

    assert run_local(deployment) == 0

    reports = json.loads(report_path.read_text())
    assert len(reports) == 2
    for report in reports:
        assert report["complete"]
        assert len(report["offices"]) == 4
        assert all(office["applied"] == 100 for office in report["offices"])
        assert all(office["completed"] for office in report["offices"])
