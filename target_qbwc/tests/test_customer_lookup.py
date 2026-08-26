"""Tests for customer FullName lookup helpers."""

from __future__ import annotations

import unittest

from target_qbwc.customer_lookup import customer_full_name_for_lookup


class TestCustomerLookup(unittest.TestCase):
    def test_top_level_customer_uses_name(self):
        payload = {"Name": "Elon Musk"}
        self.assertEqual(customer_full_name_for_lookup(payload), "Elon Musk")

    def test_subcustomer_with_parent_full_name(self):
        payload = {
            "Name": "SpaceX",
            "ParentRef": {"FullName": "Elon Musk"},
        }
        self.assertEqual(customer_full_name_for_lookup(payload), "Elon Musk:SpaceX")

    def test_subcustomer_with_parent_list_id(self):
        payload = {
            "Name": "SpaceX",
            "ParentRef": {"ListID": "80000009-1750961692"},
        }
        resolver = lambda list_id: "Elon Musk" if list_id == "80000009-1750961692" else None
        self.assertEqual(
            customer_full_name_for_lookup(payload, parent_full_name_resolver=resolver),
            "Elon Musk:SpaceX",
        )

    def test_parent_full_name_takes_precedence_over_list_id(self):
        payload = {
            "Name": "SpaceX",
            "ParentRef": {
                "FullName": "Elon Musk",
                "ListID": "80000009-1750961692",
            },
        }
        resolver = lambda list_id: "Wrong Parent"
        self.assertEqual(
            customer_full_name_for_lookup(payload, parent_full_name_resolver=resolver),
            "Elon Musk:SpaceX",
        )

    def test_already_qualified_name_is_not_doubled(self):
        payload = {
            "Name": "Elon Musk:SpaceX",
            "ParentRef": {"FullName": "Elon Musk"},
        }
        self.assertEqual(customer_full_name_for_lookup(payload), "Elon Musk:SpaceX")

    def test_list_id_lookup_falls_back_to_name_when_parent_missing(self):
        payload = {
            "Name": "SpaceX",
            "ParentRef": {"ListID": "missing-id"},
        }
        self.assertEqual(
            customer_full_name_for_lookup(payload, parent_full_name_resolver=lambda list_id: None),
            "SpaceX",
        )

    def test_list_id_lookup_without_resolver_falls_back_to_name(self):
        payload = {
            "Name": "SpaceX",
            "ParentRef": {"ListID": "80000009-1750961692"},
        }
        self.assertEqual(customer_full_name_for_lookup(payload), "SpaceX")
