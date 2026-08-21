"""Static checks for the migration Task, Job shape, and RBAC boundary."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]


def _pipeline() -> dict:
    with (ROOT / "tekton/omnigent-app-build-pipeline.yaml").open() as handle:
        return yaml.safe_load(handle)


def _task(name: str) -> dict:
    pipeline = _pipeline()
    tasks = pipeline["spec"].get("tasks", []) + pipeline["spec"].get("finally", [])
    return next(task for task in tasks if task["name"] == name)


def _task_script(name: str) -> str:
    task = _task(name)
    task_spec = task.get("taskSpec", {})
    return "\n".join(step.get("script", "") for step in task_spec.get("steps", []))


def _digest_commit_script() -> str:
    return _task_script("commit-gitops-digest")


def _digest_commit_task() -> dict:
    return _task("commit-gitops-digest")


def _digest_commit_resources() -> list[dict]:
    with (ROOT / "tekton/omnigent-github-token.yaml").open() as handle:
        return [document for document in yaml.safe_load_all(handle) if document]


def _job_manifest() -> dict:
    script = _task_script("run-migrations")
    heredoc = script.split("<<EOF\n", 1)[1].split("\nEOF\n", 1)[0]
    # The outer task shell must not expand these; the Job container does.
    heredoc = heredoc.replace(r"\$(DB_USER)", "$(DB_USER)")
    heredoc = heredoc.replace(r"\$(DB_PASSWORD)", "$(DB_PASSWORD)")
    return yaml.safe_load(heredoc)


def _trigger_pipeline_run() -> dict:
    with (ROOT / "tekton/trigger.yaml").open() as handle:
        for document in yaml.safe_load_all(handle):
            if not document:
                continue
            for resource in document.get("spec", {}).get("resourcetemplates", []):
                if resource.get("kind") == "PipelineRun":
                    return resource
    raise AssertionError("trigger.yaml has no PipelineRun resource template")


def test_migration_task_runs_after_build_without_rollout() -> None:
    pipeline = _pipeline()
    migration = _task("run-migrations")

    assert migration["runAfter"] == ["build-and-push"]
    task_names = {
        task["name"]
        for task in pipeline["spec"].get("tasks", []) + pipeline["spec"].get("finally", [])
    }
    assert "rollout-restart" not in task_names


def test_gitops_commit_runs_only_after_build_and_migrations_succeed() -> None:
    pipeline = _pipeline()["spec"]
    commit = _task("commit-gitops-digest")

    assert commit["runAfter"] == ["build-and-push", "run-migrations"]
    assert commit in pipeline["tasks"]
    assert commit not in pipeline.get("finally", [])
    assert commit["params"][0] == {
        "name": "image-digest",
        "value": "$(tasks.build-and-push.results.IMAGE_DIGEST)",
    }


def test_gitops_commit_is_unconditional_and_requires_credentials() -> None:
    script = _digest_commit_script()
    task = _digest_commit_task()
    credentials = (ROOT / "tekton/omnigent-github-token.yaml").read_text()

    assert "env" not in task["taskSpec"]["steps"][0]
    assert "AUTO_DEPLOY_ENABLED" not in script
    assert "Auto-deploy disabled" not in script
    assert "Missing token in Secret omnigent-gitops-write-credentials" in script
    assert "VaultDynamicSecret" in credentials
    assert "ExternalSecret" in credentials


def test_gitops_commit_has_no_cluster_deployment_patch_path() -> None:
    script = _digest_commit_script()
    resources = _digest_commit_resources()

    assert "kubectl" not in script
    assert not any(
        resource["kind"] in {"Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding"}
        for resource in resources
    )
    assert "gitops-credentials" in script
    assert "omnigent-gitops-write-credentials" in script


def test_gitops_commit_credentials_mount_is_outside_tekton_and_matches_script() -> None:
    task = _digest_commit_task()
    step = task["taskSpec"]["steps"][0]
    mounts = step["volumeMounts"]
    script = _digest_commit_script()

    assert len(mounts) == 1
    mount = mounts[0]
    mount_path = mount["mountPath"]
    assert mount["name"] == "gitops-credentials"
    assert mount_path == "/workspace/gitops-credentials"
    assert not (mount_path == "/tekton" or mount_path.startswith("/tekton/"))
    assert f"CREDENTIAL_DIR={mount_path}" in script
    assert "${CREDENTIAL_DIR}/token" in script
    assert "/tekton/gitops-credentials" not in script


def test_gitops_digest_edit_preserves_proxy_registry_and_only_changes_digest() -> None:
    script = _digest_commit_script()

    assert (
        'TARGET_IMAGE="repos.eu-central-1.myidp.cloud/$(params.image-namespace)/$(params.image-name)"'
        in script
    )
    assert "repos-direct.eu-central-1.myidp.cloud" not in script
    assert "TARGET_RELATIVE=platform/apps/omnigent/manifests/05-deployment.yaml" in script
    assert "@sha256:[0-9a-fA-F]{64}" in script
    assert "s|@sha256:[0-9a-fA-F]{64}|@${IMAGE_DIGEST}|" in script
    assert "NORMALIZED_OLD_LINE" in script
    assert "NORMALIZED_UPDATED_LINE" in script


def test_gitops_commit_marks_schema_changes_and_rejects_plain_revert() -> None:
    script = _digest_commit_script()

    assert "diff --diff-filter=A --name-only" in script
    assert "omnigent/db/migrations/versions/*.py" in script
    assert "SCHEMA CHANGE: new Alembic revision" in script
    assert "plain digest revert is unsafe" in script
    assert "roll forward to a compatible image" in script


def test_gitops_commit_rebases_with_bounded_non_force_pushes() -> None:
    script = _digest_commit_script()

    assert "MAX_PUSH_ATTEMPTS=5" in script
    assert 'git -C "$GITOPS_DIR" fetch origin main' in script
    assert 'git -C "$GITOPS_DIR" rebase origin/main' in script
    assert 'git -C "$GITOPS_DIR" push origin HEAD:main' in script
    assert "--force" not in script
    assert "--force-with-lease" not in script


def test_job_image_is_digest_pinned() -> None:
    job = _job_manifest()
    image = job["spec"]["template"]["spec"]["containers"][0]["image"]
    migration = _task("run-migrations")

    assert image.endswith("@$(params.image-digest)")
    assert ":$(params.image-tag)" not in image
    assert migration["params"] == [
        {"name": "image-digest", "value": "$(tasks.build-and-push.results.IMAGE_DIGEST)"}
    ]
    assert migration["taskSpec"]["params"] == [{"name": "image-digest", "type": "string"}]


def test_job_env_is_exactly_what_migrate_only_reads() -> None:
    script = _task_script("run-migrations")
    container = _job_manifest()["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item for item in container["env"]}

    assert set(env) == {"DB_USER", "DB_PASSWORD", "DATABASE_URL"}
    assert container["envFrom"] == [{"configMapRef": {"name": "omnigent-config"}}]
    assert env["DB_USER"]["valueFrom"]["secretKeyRef"] == {
        "name": "omnigent-db-app",
        "key": "username",
    }
    assert env["DB_PASSWORD"]["valueFrom"]["secretKeyRef"] == {
        "name": "omnigent-db-app",
        "key": "password",
    }
    assert env["DATABASE_URL"]["value"].startswith("postgresql+psycopg://")
    assert "$(DB_USER)" in env["DATABASE_URL"]["value"]
    assert "$(DB_PASSWORD)" in env["DATABASE_URL"]["value"]
    assert r"\$(DB_USER)" in script
    assert r"\$(DB_PASSWORD)" in script
    assert "omnigent-oidc-secret" not in script


def test_migration_task_job_has_prompt_failure_and_runtime_command() -> None:
    job = _job_manifest()
    spec = job["spec"]
    pod_spec = spec["template"]["spec"]
    container = pod_spec["containers"][0]

    assert job["metadata"]["generateName"] == "omnigent-migrate-"
    assert spec["backoffLimit"] == 1
    assert spec["activeDeadlineSeconds"] == 900
    assert spec["ttlSecondsAfterFinished"] == 3600
    assert pod_spec["restartPolicy"] == "Never"
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["imagePullSecrets"] == [{"name": "harbor-registry-credentials"}]
    assert container["command"] == ["python", "/app/entrypoint.py", "--migrate-only"]
    assert "restartPolicy" not in spec
    assert "containers" not in spec


def test_poll_fails_fast_on_pod_startup_errors() -> None:
    script = _task_script("run-migrations")
    assert 'JOB_NAME="${JOB##*/}"' in script
    assert "status.containerStatuses[*].state.waiting.reason" in script
    for reason in (
        "ImagePullBackOff",
        "ErrImagePull",
        "InvalidImageName",
        "CreateContainerConfigError",
    ):
        assert reason in script
    assert "Migration Job pod failed to start" in script


def test_migration_task_observes_success_and_failure_and_preserves_status() -> None:
    script = _task_script("run-migrations")
    assert '.type=="Complete"' in script
    assert '.type=="Failed"' in script
    assert "remaining=930" in script
    assert 'kubectl logs --namespace omnigent "${JOB}" || true' in script
    assert 'kubectl delete --namespace omnigent "${JOB}" --ignore-not-found || true' in script


def test_pipeline_does_not_wait_for_rollout() -> None:
    pipeline = _pipeline()["spec"]
    task_names = {task["name"] for task in pipeline.get("tasks", []) + pipeline.get("finally", [])}
    assert "rollout-restart" not in task_names


def test_rollout_rbac_is_removed() -> None:
    assert not (ROOT / "tekton/rollout-rbac.yaml").exists()
    assert "rollout-rbac.yaml" not in (ROOT / "tekton/kustomization.yaml").read_text()


def test_every_kubectl_task_has_a_service_account_mapping() -> None:
    pipeline = _pipeline()["spec"]
    kubectl_tasks = {
        task["name"]
        for task in pipeline.get("tasks", []) + pipeline.get("finally", [])
        if "kubectl" in _task_script(task["name"])
    }
    pipeline_run = _trigger_pipeline_run()
    mapped_tasks = {
        spec["pipelineTaskName"] for spec in pipeline_run["spec"].get("taskRunSpecs", [])
    }
    assert mapped_tasks == kubectl_tasks
    assert mapped_tasks == {"run-migrations"}


def test_job_command_matches_dockerfile_entrypoint_path() -> None:
    dockerfile = (ROOT / "deploy/docker/Dockerfile").read_text()
    command = _job_manifest()["spec"]["template"]["spec"]["containers"][0]["command"]
    destination = next(
        line.split()[-1]
        for line in dockerfile.splitlines()
        if line.startswith("COPY deploy/docker/entrypoint.py ")
    )
    assert command[1] == destination


def test_migrate_rbac_grants_no_secret_access() -> None:
    documents = list(yaml.safe_load_all((ROOT / "tekton/migrate-rbac.yaml").open()))
    assert all(
        "secrets" not in rule.get("resources", [])
        for document in documents
        for rule in document.get("rules", [])
    )


def test_migrate_service_account_is_in_kustomization() -> None:
    text = (ROOT / "tekton/kustomization.yaml").read_text()
    assert "migrate-rbac.yaml" in text
