from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CyberItPreset:
    identifier: str
    label: str
    description: str
    disclaimer: str
    naics_codes: tuple[str, ...]
    psc_codes: tuple[str, ...]
    keywords: tuple[str, ...]
    award_type_codes: tuple[str, ...] = ("A", "B", "C", "D")


CYBER_IT_V1 = CyberItPreset(
    identifier="cyber-it-v1",
    label="Cyber/IT preset",
    description="A configurable union of selected IT NAICS codes, PSC codes, and cybersecurity keywords.",
    disclaimer="This preset is a market-research filter and does not identify all cybersecurity spending.",
    naics_codes=("518210", "541511", "541512", "541513", "541519"),
    psc_codes=("D301", "D302", "D306", "D307", "D310", "D311", "D317", "D318", "D319", "D320", "D321", "D399"),
    keywords=(
        "cybersecurity",
        "cyber security",
        "zero trust",
        "information security",
        "information assurance",
        "incident response",
        "penetration testing",
        "threat hunting",
        "vulnerability management",
    ),
)


def get_preset(identifier: str = "cyber-it-v1") -> CyberItPreset:
    if identifier != CYBER_IT_V1.identifier:
        raise ValueError("Unknown USAspending preset")
    return CYBER_IT_V1


def as_public_dict(preset: CyberItPreset = CYBER_IT_V1) -> dict[str, object]:
    return {
        "id": preset.identifier,
        "label": preset.label,
        "description": preset.description,
        "disclaimer": preset.disclaimer,
        "rules": {
            "award_type_codes": list(preset.award_type_codes),
            "naics_codes": list(preset.naics_codes),
            "psc_codes": list(preset.psc_codes),
            "keywords": list(preset.keywords),
        },
    }
