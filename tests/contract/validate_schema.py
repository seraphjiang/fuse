#!/usr/bin/env python3
"""Validate JSON from stdin against an OpenAPI 3.1 component schema.
Handles OpenAPI nullable:true by converting to JSON Schema type arrays.
Usage: echo '{}' | python3 validate_schema.py SPEC SCHEMA_NAME [--array]
Exit 0=valid, 1=invalid, 2=error.
"""
import json, sys, yaml, copy
from jsonschema import validate, ValidationError, RefResolver

def patch_nullable(obj):
    """Convert OpenAPI nullable:true to JSON Schema {"type":["T","null"]}."""
    if not isinstance(obj, dict):
        return obj
    for k, v in list(obj.items()):
        if isinstance(v, dict):
            if v.get("nullable") and "type" in v:
                v = copy.deepcopy(v)
                v["type"] = [v["type"], "null"]
                del v["nullable"]
                obj[k] = v
            patch_nullable(v)
        elif isinstance(v, list):
            for item in v:
                patch_nullable(item) if isinstance(item, dict) else None
    return obj

def main():
    spec_path, name = sys.argv[1], sys.argv[2]
    as_array = "--array" in sys.argv
    with open(spec_path) as f:
        spec = yaml.safe_load(f)
    patch_nullable(spec.get("components", {}).get("schemas", {}))
    ref = {"$ref": f"#/components/schemas/{name}"}
    if as_array:
        ref = {"type": "array", "items": ref}
    try:
        validate(instance=json.load(sys.stdin), schema=ref, resolver=RefResolver.from_schema(spec))
    except ValidationError as e:
        print(f"FAIL: {e.message}")
        sys.exit(1)

if __name__ == "__main__":
    main()
