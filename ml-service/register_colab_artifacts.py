import json
from pathlib import Path

from sqlalchemy import text

from db import get_engine


ARTIFACT_DIR = Path("artifacts")
MANIFEST_PATH = ARTIFACT_DIR / "registry_manifest.json"


def normalize_metrics(metrics):
    return {
        "accuracy_score": metrics.get("accuracy_score"),
        "precision_score": metrics.get("precision_score"),
        "recall_score": metrics.get("recall_score"),
        "f1_score": metrics.get("f1_score"),
        "silhouette_score": metrics.get("silhouette_score"),
        "rouge_score": metrics.get("rouge_score"),
    }


def main():
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"Manifest not found: {MANIFEST_PATH}")

    with open(MANIFEST_PATH, "r", encoding="utf-8") as file:
        manifest = json.load(file)

    engine = get_engine()

    with engine.begin() as conn:
        for item in manifest:
            model_name = item["model_name"]
            metrics = normalize_metrics(item.get("metrics", {}))

            conn.execute(
                text(
                    """
                    UPDATE ml_model_registry
                    SET is_active = 0,
                        deprecated_at = NOW()
                    WHERE model_name = :model_name
                      AND is_active = 1
                    """
                ),
                {"model_name": model_name},
            )

            conn.execute(
                text(
                    """
                    INSERT INTO ml_model_registry (
                        model_name,
                        model_type,
                        version,
                        artifact_path,
                        artifact_size_bytes,
                        accuracy_score,
                        precision_score,
                        recall_score,
                        f1_score,
                        silhouette_score,
                        rouge_score,
                        training_data_size,
                        training_duration_sec,
                        hyperparameters,
                        feature_columns,
                        trained_at,
                        deployed_at,
                        is_active,
                        notes
                    )
                    VALUES (
                        :model_name,
                        :model_type,
                        :version,
                        :artifact_path,
                        :artifact_size_bytes,
                        :accuracy_score,
                        :precision_score,
                        :recall_score,
                        :f1_score,
                        :silhouette_score,
                        :rouge_score,
                        :training_data_size,
                        :training_duration_sec,
                        :hyperparameters,
                        :feature_columns,
                        NOW(),
                        NOW(),
                        1,
                        :notes
                    )
                    """
                ),
                {
                    "model_name": item["model_name"],
                    "model_type": item["model_type"],
                    "version": item["version"],
                    "artifact_path": item["artifact_path"],
                    "artifact_size_bytes": item["artifact_size_bytes"],
                    "accuracy_score": metrics["accuracy_score"],
                    "precision_score": metrics["precision_score"],
                    "recall_score": metrics["recall_score"],
                    "f1_score": metrics["f1_score"],
                    "silhouette_score": metrics["silhouette_score"],
                    "rouge_score": metrics["rouge_score"],
                    "training_data_size": item["training_data_size"],
                    "training_duration_sec": 0,
                    "hyperparameters": json.dumps(item.get("hyperparameters", {}), ensure_ascii=False),
                    "feature_columns": json.dumps(item.get("feature_columns", []), ensure_ascii=False),
                    "notes": item.get("notes"),
                },
            )

            print(f"[REGISTERED] {model_name} {item['version']}")

    print("[DONE] All Colab artifacts registered to ml_model_registry.")


if __name__ == "__main__":
    main()