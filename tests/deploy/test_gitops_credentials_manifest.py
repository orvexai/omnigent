from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]


def _documents(path: Path) -> list[dict]:
    return [doc for doc in yaml.safe_load_all(path.read_text()) if doc]


def test_gitops_credentials_use_short_lived_openbao_github_app_token() -> None:
    documents = _documents(ROOT / "tekton/omnigent-github-token.yaml")

    assert [(doc["kind"], doc["metadata"]["name"]) for doc in documents] == [
        ("ServiceAccount", "omnigent-github-token"),
        ("VaultDynamicSecret", "omnigent-github-token-generator"),
        ("ExternalSecret", "omnigent-gitops-write-credentials"),
    ]
    generator = documents[1]
    assert generator["spec"]["path"] == "github/token/crew-write"
    assert generator["spec"]["provider"]["auth"]["kubernetes"]["role"] == ("omnigent-github-token")
    external_secret = documents[2]
    assert external_secret["spec"]["refreshInterval"] == "25m"
    assert external_secret["spec"]["target"]["template"]["data"] == {"token": "{{ .token }}"}
