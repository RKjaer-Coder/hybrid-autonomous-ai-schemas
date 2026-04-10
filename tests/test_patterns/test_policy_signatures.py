from __future__ import annotations

import unittest

from immune.patterns.policy_signatures import (
    CONSTRUCTION_ALLOWLIST,
    ResourceLimits,
    ResourceUsage,
    check_deny_rules,
    check_resource_limits,
    check_trust_tier,
)


class TestPolicySignatures(unittest.TestCase):
    def test_endpoint_allowlist_localhost_pass(self):
        self.assertTrue(CONSTRUCTION_ALLOWLIST.is_permitted("http://localhost:11434/v1"))

    def test_endpoint_allowlist_external_block(self):
        self.assertFalse(CONSTRUCTION_ALLOWLIST.is_permitted("https://evil.com:443/x"))

    def test_endpoint_allowlist_wrong_port_block(self):
        self.assertFalse(CONSTRUCTION_ALLOWLIST.is_permitted("https://localhost:443/x"))

    def test_resource_limits_within(self):
        limits = ResourceLimits(max_tool_calls=10, max_memory_writes=2, max_api_spend_usd=1.0)
        usage = ResourceUsage(tool_calls=8, memory_writes=2, api_spend_usd=1.0)
        self.assertIsNone(check_resource_limits(limits, usage))

    def test_resource_limits_tool_calls_exceed(self):
        limits = ResourceLimits(max_tool_calls=10)
        usage = ResourceUsage(tool_calls=11)
        self.assertIn("max_tool_calls", check_resource_limits(limits, usage) or "")

    def test_resource_limits_spend_exceed(self):
        limits = ResourceLimits(max_api_spend_usd=0.0)
        usage = ResourceUsage(api_spend_usd=0.1)
        self.assertIn("max_api_spend_usd", check_resource_limits(limits, usage) or "")

    def test_trust_tier_tier4_shell_block(self):
        self.assertIsNotNone(check_trust_tier("shell_command", 4))

    def test_trust_tier_tier1_shell_pass(self):
        self.assertIsNone(check_trust_tier("shell_command", 1))

    def test_trust_tier_tier3_memory_write_block(self):
        self.assertIsNotNone(check_trust_tier("memory_write", 3))

    def test_deny_rule_rm_root_block(self):
        result = check_deny_rules("shell_command", "rm -rf /")
        self.assertIsNotNone(result)

    def test_deny_rule_rm_relative_pass(self):
        self.assertIsNone(check_deny_rules("shell_command", "rm -rf ./temp/"))

    def test_deny_rule_write_system_path_block(self):
        result = check_deny_rules("file_write", "/etc/passwd")
        self.assertIsNotNone(result)

    def test_deny_rule_write_tmp_pass(self):
        self.assertIsNone(check_deny_rules("file_write", "/tmp/output.txt"))

    def test_endpoint_allowlist_scheme_block(self):
        self.assertFalse(CONSTRUCTION_ALLOWLIST.is_permitted("ftp://localhost:11434/data"))

    def test_resource_limits_memory_exceed(self):
        limits = ResourceLimits(max_memory_writes=1)
        usage = ResourceUsage(memory_writes=2)
        self.assertIn("max_memory_writes", check_resource_limits(limits, usage) or "")

    def test_deny_rule_world_writable_block(self):
        self.assertIsNotNone(check_deny_rules("shell_command", "chmod 777 file.txt"))


if __name__ == "__main__":
    unittest.main()
