"""Unit tests for mage_cli helper functions — no Mage instance required."""

import hashlib
import json
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# mage_cli.py re-exports flow_name_to_pipeline_uuid from mage_compiler;
# import directly from the canonical source.
from metaflow_extensions.mage.plugins.mage.mage_compiler import flow_name_to_pipeline_uuid


class TestFlowNameToPipelineUuid:
    def test_basic(self):
        assert flow_name_to_pipeline_uuid("HelloFlow") == "helloflow"

    def test_case_insensitive(self):
        assert flow_name_to_pipeline_uuid("HELLOFLOW") == "helloflow"

    def test_dash(self):
        assert flow_name_to_pipeline_uuid("My-Flow") == "my_flow"

    def test_collision_documented(self):
        """Explicit test that the collision exists — so any future fix is visible."""
        a = flow_name_to_pipeline_uuid("My-Flow")
        b = flow_name_to_pipeline_uuid("My_Flow")
        # Currently these collide — if this assertion changes, the collision is fixed
        assert a == b, "UUID collision between My-Flow and My_Flow — this is a known bug (D-UUID-1)"
