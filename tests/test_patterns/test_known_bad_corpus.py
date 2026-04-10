from __future__ import annotations

import unittest

from immune.patterns.encoding_detector import decode_and_recheck, detect_encodings
from immune.patterns.ipi_patterns import check_ipi
from immune.patterns.known_bad_corpus import KNOWN_BAD_CORPUS


class TestKnownBadCorpus(unittest.TestCase):
    """ZERO TOLERANCE: Every known-bad payload must be caught."""

    def test_all_known_bad_payloads_detected(self):
        for payload in KNOWN_BAD_CORPUS:
            with self.subTest(payload_id=payload.payload_id):
                if payload.encoding:
                    detections = detect_encodings(payload.payload, payload.expected_field)
                    recheck_matches = decode_and_recheck(detections)
                    self.assertTrue(
                        len(recheck_matches) > 0,
                        f"{payload.payload_id}: Encoded payload not detected after decode",
                    )
                else:
                    matches = check_ipi(payload.payload)
                    self.assertTrue(
                        len(matches) > 0,
                        f"{payload.payload_id}: Payload not detected: {payload.description}",
                    )


if __name__ == "__main__":
    unittest.main()
