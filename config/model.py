from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import Tuple


@dataclass
class ModelConfig:
    day_parts: Dict[str, float] = field(
        default_factory=lambda: {
            'Night': 0.8,
            'Early Morning': 0.9,
            'Morning': 1.1,
            'Early Afternoon': 1.0,
            'Afternoon': 1.1,
            'Early Evening': 1.2,
            'Evening': 1.3,
            'Early Night': 1.0
        }
    )

    time_periods_6hr: Dict[str, tuple] = field(
        default_factory=lambda: {
            'Night': (0, 6),
            'Morning': (6, 12),
            'Afternoon': (12, 18),
            'Evening': (18, 24)
        }
    )
    time_periods_3hr: Dict[str, tuple] = field(
        default_factory=lambda: {
            'Night': (0, 3),
            'Early Morning': (3, 6),
            'Morning': (6, 9),
            'Early Afternoon': (9, 12),
            'Afternoon': (12, 15),
            'Early Evening': (15, 18),
            'Evening': (18, 21),
            'Early Night': (21, 24)
        }
    )

    max_ratio_bins: Dict[Tuple[float, float], float] = field(
        default_factory=lambda: {
            (0, 0.075): 1.0,
            (0.075, 0.25): 1.1,
            (0.25, 0.45): 1.3,
            (0.45, 0.95): 1.5,
            (0.95, 1.0): 2
        }
    )
    mean_ratio_bins: Dict[Tuple[float, float], float] = field(
        default_factory=lambda: {
            (0, 0.015): 1.0,
            (0.015, 0.05): 1.1,
            (0.05, 0.1): 1.2,
            (0.1, 0.2): 1.5,
            (0.2, 1.0): 2.0
        }
    )
