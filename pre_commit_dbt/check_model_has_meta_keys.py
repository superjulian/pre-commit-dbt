import argparse
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_missing_file_paths
from pre_commit_dbt.utils import get_model_schemas
from pre_commit_dbt.utils import get_model_sqls
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import JsonOpenError


def validate_keys(
    actual: Iterable[str], expected: Iterable[str], allow_extra_keys: bool
) -> bool:
    actual = set(actual)
    expected = set(expected)
    if allow_extra_keys:
        return expected.issubset(actual)
    else:
        return expected == actual

def logger (node):
    print ("id: ", node.get("unique_id"))
    print ("resource_type: ", node.get("resource_type"))
    print ("meta: ", node.get("meta"))
    return True

def has_meta_key(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    meta_keys: Sequence[str],
    allow_extra_keys: bool,
) -> int:
    paths = get_missing_file_paths(paths, manifest)
    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = {
        key 
        for key,value in sqls.items()
        #exclude models in test directory
        if str(value).split("/")[0] != "tests"
    }
    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
    # convert to set
    in_models = {
        model.filename
        for model in models
        if validate_keys(model.node.get("meta", {}).keys(), meta_keys, allow_extra_keys)
    }
    missing = filenames.difference(in_models)
    for model in missing:
        status_code = 1
        result = "\n- ".join(list(meta_keys))  # pragma: no mutate
        print(
            f"{sqls.get(model)}: "
            f"does not have some of the meta keys defined:\n- {result}",
        )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    parser.add_argument(
        "--meta-keys",
        nargs="+",
        required=True,
        help="List of required key in meta part of model.",
    )

    parser.add_argument(
        "--allow-extra-keys",
        action="store_true",
        required=False,
        help="Whether extra keys are allowed.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    return has_meta_key(
        paths=args.filenames,
        manifest=manifest,
        meta_keys=args.meta_keys,
        allow_extra_keys=args.allow_extra_keys,
    )


if __name__ == "__main__":
    exit(main())
